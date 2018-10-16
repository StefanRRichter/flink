/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * TODO.
 */
public class Scheduler implements SlotProvider, SlotOwner {

	private static final CompletableFuture<LogicalSlot> CHAIN_ROOT = CompletableFuture.completedFuture(null);

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Nonnull
	private final SlotSelectionStrategy slotSelectionStrategy;

	/** Managers for the different slot sharing groups. */
	@Nonnull
	private final Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagersMap;

	@Nonnull
	private final SlotPoolGateway slotPoolGateway;

	@Nonnull
	private CompletableFuture<LogicalSlot> allocationQueue;

	@Nonnull
	private final Executor componentMainThreadExecutor;

	public Scheduler(
		@Nonnull Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagersMap,
		@Nonnull SlotSelectionStrategy slotSelectionStrategy,
		@Nonnull SlotPoolGateway slotPoolGateway) {

		this.slotSelectionStrategy = slotSelectionStrategy;
		this.slotSharingManagersMap = slotSharingManagersMap;
		this.slotPoolGateway = slotPoolGateway;
		this.allocationQueue = CHAIN_ROOT;
		this.componentMainThreadExecutor = Executors.newSingleThreadExecutor();
	}

	//---------------------------

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		boolean allowQueuedScheduling,
		SlotProfile slotProfile,
		Time allocationTimeout) {
		log.debug("Received slot request [{}] for task: {}", slotRequestId, scheduledUnit.getTaskToExecute());

		allocationQueue = allocationQueue.thenComposeAsync((ignored) -> scheduledUnit.getSlotSharingGroupId() == null ?
				allocateSingleSlot(slotRequestId, slotProfile, allowQueuedScheduling, allocationTimeout) :
				allocateSharedSlot(slotRequestId, scheduledUnit, slotProfile, allowQueuedScheduling, allocationTimeout),
			componentMainThreadExecutor);
		return allocationQueue;
	}

	@Override
	public CompletableFuture<Acknowledge> cancelSlotRequest(
		SlotRequestId slotRequestId,
		@Nullable SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {

		if (slotSharingGroupId != null) {
			return CompletableFuture.supplyAsync(() -> releaseSharedSlot(slotRequestId, slotSharingGroupId, cause), componentMainThreadExecutor);
		} else {
			return slotPoolGateway.releaseSlot(slotRequestId, cause);
		}
	}

	@Override
	public CompletableFuture<Boolean> returnAllocatedSlot(LogicalSlot logicalSlot) {

		SlotRequestId slotRequestId = logicalSlot.getSlotRequestId();
		SlotSharingGroupId slotSharingGroupId = logicalSlot.getSlotSharingGroupId();
		FlinkException cause = new FlinkException("Slot is being returned to the SlotPool.");

		CompletableFuture<Acknowledge> releaseSlotFuture = cancelSlotRequest(slotRequestId, slotSharingGroupId, cause);
		return releaseSlotFuture.thenApply((Acknowledge acknowledge) -> true);
	}

	//---------------------------

	private CompletableFuture<LogicalSlot> allocateSingleSlot(
		SlotRequestId slotRequestId,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		Time allocationTimeout) {

		CompletableFuture<SlotAndLocality> availableAllocationFuture =
			tryAllocateFromAvailable(slotRequestId, slotProfile);

		CompletableFuture<SlotAndLocality> availableOrNewAllocationFuture =
			availableAllocationFuture.thenCompose((SlotAndLocality slotAndLocality) -> {

				if (slotAndLocality != null) {
					// already successful from available
					return CompletableFuture.completedFuture(slotAndLocality);
				} else if (allowQueuedScheduling) {
					// we allocate by requesting a new slot
					return slotPoolGateway
						.requestNewAllocatedSlot(slotRequestId, slotProfile.getResourceProfile(), allocationTimeout)
						.thenApply((AllocatedSlot allocatedSlot) -> new SlotAndLocality(allocatedSlot, Locality.UNKNOWN));
				} else {
					// failed to allocate
					return FutureUtils.completedExceptionally(
						new NoResourceAvailableException("Could not allocate a simple slot for " + slotRequestId + '.'));
				}
			});

		return availableOrNewAllocationFuture.thenCompose((SlotAndLocality slotAndLocality) -> {
			final AllocatedSlot allocatedSlot = slotAndLocality.getSlot();

			final SingleLogicalSlot singleTaskSlot = new SingleLogicalSlot(
				slotRequestId,
				allocatedSlot,
				null,
				slotAndLocality.getLocality(),
				this); //TODO!!!!!!!!!

			if (allocatedSlot.tryAssignPayload(singleTaskSlot)) {
				return CompletableFuture.completedFuture(singleTaskSlot);
			} else {
				final FlinkException flinkException =
					new FlinkException("Could not assign payload to allocated slot " + allocatedSlot.getAllocationId() + '.');
				return slotPoolGateway
					.releaseSlot(slotRequestId, flinkException)
					.thenCompose((ack) -> {
						throw new CompletionException(flinkException);
					});
			}
		});
	}

	private CompletableFuture<SlotAndLocality> tryAllocateFromAvailable(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotProfile slotProfile) {

		CompletableFuture<List<SlotInfo>> slotsInfoFuture = slotPoolGateway.getAvailableSlotsInformation();

		return slotsInfoFuture.thenCompose((List<SlotInfo> slotInfoList) -> {

			SlotSelectionStrategy.SlotInfoAndLocality selectedAvailableSlot =
				slotSelectionStrategy.selectBestSlotForProfile(slotInfoList, slotProfile);

			SlotInfo selectedSlotInfo = selectedAvailableSlot.getSlotInfo();

			if (selectedSlotInfo != null) {
				return slotPoolGateway
					.allocateAvailableSlot(slotRequestId, selectedSlotInfo.getAllocationId())
					.thenCompose((AllocatedSlot allocatedSlot) -> {
						if (allocatedSlot != null) {
							// success
							return CompletableFuture.completedFuture(
								new SlotAndLocality(allocatedSlot, selectedAvailableSlot.getLocality()));
						} else {
							// seems like the info is outdated because the allocation failed, retry...
							return tryAllocateFromAvailable(slotRequestId, slotProfile);
						}
					});
			}

			// no available slot fits
			return CompletableFuture.completedFuture(null);
		});
	}

	// ------------------------------- slot sharing code

	private CompletableFuture<LogicalSlot> allocateSharedSlot(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		Time allocationTimeout) {
		// allocate slot with slot sharing
		final SlotSharingManager multiTaskSlotManager = slotSharingManagersMap.computeIfAbsent(
			scheduledUnit.getSlotSharingGroupId(),
			id -> new SlotSharingManager(
				id,
				slotPoolGateway, //TODO was "this", aka slot pool
				this)); //TODO

		final CompletableFuture<SlotSharingManager.MultiTaskSlotLocality> multiTaskSlotLocalityFuture;

		if (scheduledUnit.getCoLocationConstraint() != null) {
			multiTaskSlotLocalityFuture = allocateCoLocatedMultiTaskSlot(
				scheduledUnit.getCoLocationConstraint(),
				multiTaskSlotManager,
				slotProfile,
				allowQueuedScheduling,
				allocationTimeout);
		} else {
			multiTaskSlotLocalityFuture = allocateMultiTaskSlot(
				scheduledUnit.getJobVertexId(),
				multiTaskSlotManager,
				slotProfile,
				allowQueuedScheduling,
				allocationTimeout);
		}

		return multiTaskSlotLocalityFuture.thenComposeAsync((SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality) -> {

				// sanity check
				Preconditions.checkState(!multiTaskSlotLocality.getMultiTaskSlot().contains(scheduledUnit.getJobVertexId()));

				final SlotSharingManager.SingleTaskSlot leaf = multiTaskSlotLocality.getMultiTaskSlot().allocateSingleTaskSlot(
					slotRequestId,
					scheduledUnit.getJobVertexId(),
					multiTaskSlotLocality.getLocality());
				return leaf.getLogicalSlotFuture();
			}
		, componentMainThreadExecutor);
	}

	/**
	 * Allocates a co-located {@link SlotSharingManager.MultiTaskSlot} for the given {@link CoLocationConstraint}.
	 *
	 * <p>If allowQueuedScheduling is true, then the returned {@link SlotSharingManager.MultiTaskSlot} can be
	 * uncompleted.
	 *
	 * @param coLocationConstraint for which to allocate a {@link SlotSharingManager.MultiTaskSlot}
	 * @param multiTaskSlotManager responsible for the slot sharing group for which to allocate the slot
	 * @param slotProfile specifying the requirements for the requested slot
	 * @param allowQueuedScheduling true if queued scheduling (the returned task slot must not be completed yet) is allowed, otherwise false
	 * @param allocationTimeout timeout before the slot allocation times out
	 * @return A {@link SlotAndLocality} which contains the allocated{@link SlotSharingManager.MultiTaskSlot}
	 * 		and its locality wrt the given location preferences
	 */
	private CompletableFuture<SlotSharingManager.MultiTaskSlotLocality> allocateCoLocatedMultiTaskSlot(
		CoLocationConstraint coLocationConstraint,
		SlotSharingManager multiTaskSlotManager,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		Time allocationTimeout) {
		final SlotRequestId coLocationSlotRequestId = coLocationConstraint.getSlotRequestId();

		if (coLocationSlotRequestId != null) {
			// we have a slot assigned --> try to retrieve it
			final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(coLocationSlotRequestId);

			if (taskSlot != null) {
				Preconditions.checkState(taskSlot instanceof SlotSharingManager.MultiTaskSlot);
				return CompletableFuture.completedFuture(
					SlotSharingManager.MultiTaskSlotLocality.of(((SlotSharingManager.MultiTaskSlot) taskSlot), Locality.LOCAL));
			} else {
				// the slot may have been cancelled in the mean time
				coLocationConstraint.setSlotRequestId(null);
			}
		}

		if (coLocationConstraint.isAssigned()) {
			// refine the preferred locations of the slot profile
			slotProfile = new SlotProfile(
				slotProfile.getResourceProfile(),
				Collections.singleton(coLocationConstraint.getLocation()),
				slotProfile.getPriorAllocations());
		}

		// get a new multi task slot
		return allocateMultiTaskSlot(
			coLocationConstraint.getGroupId(),
			multiTaskSlotManager,
			slotProfile,
			allowQueuedScheduling,
			allocationTimeout).thenCompose((SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality) -> {

			// check whether we fulfill the co-location constraint
			if (coLocationConstraint.isAssigned() && multiTaskSlotLocality.getLocality() != Locality.LOCAL) {
				multiTaskSlotLocality.getMultiTaskSlot().release(
					new FlinkException("Multi task slot is not local and, thus, does not fulfill the co-location constraint."));

				return FutureUtils.completedExceptionally(new NoResourceAvailableException("Could not allocate a local multi task slot for the " +
					"co location constraint " + coLocationConstraint + '.'));
			}

			final SlotRequestId slotRequestId = new SlotRequestId();
			final SlotSharingManager.MultiTaskSlot coLocationSlot =
				multiTaskSlotLocality.getMultiTaskSlot().allocateMultiTaskSlot(
					slotRequestId,
					coLocationConstraint.getGroupId());

			// mark the requested slot as co-located slot for other co-located tasks
			coLocationConstraint.setSlotRequestId(slotRequestId);

			// lock the co-location constraint once we have obtained the allocated slot
			coLocationSlot.getSlotContextFuture().whenComplete(
				(SlotContext slotContext, Throwable throwable) -> {
					if (throwable == null) {
						// check whether we are still assigned to the co-location constraint
						if (Objects.equals(coLocationConstraint.getSlotRequestId(), slotRequestId)) {
							coLocationConstraint.lockLocation(slotContext.getTaskManagerLocation());
						} else {
							log.debug("Failed to lock colocation constraint {} because assigned slot " +
									"request {} differs from fulfilled slot request {}.",
								coLocationConstraint.getGroupId(),
								coLocationConstraint.getSlotRequestId(),
								slotRequestId);
						}
					} else {
						log.debug("Failed to lock colocation constraint {} because the slot " +
								"allocation for slot request {} failed.",
							coLocationConstraint.getGroupId(),
							coLocationConstraint.getSlotRequestId(),
							throwable);
					}
				});

			return CompletableFuture.completedFuture(
				SlotSharingManager.MultiTaskSlotLocality.of(coLocationSlot, multiTaskSlotLocality.getLocality()));
		});
	}

	/**
	 * Allocates a {@link SlotSharingManager.MultiTaskSlot} for the given groupId which is in the
	 * slot sharing group for which the given {@link SlotSharingManager} is responsible.
	 *
	 * <p>If allowQueuedScheduling is true, then the method can return an uncompleted {@link SlotSharingManager.MultiTaskSlot}.
	 *
	 * @param groupId for which to allocate a new {@link SlotSharingManager.MultiTaskSlot}
	 * @param slotSharingManager responsible for the slot sharing group for which to allocate the slot
	 * @param slotProfile slot profile that specifies the requirements for the slot
	 * @param allowQueuedScheduling true if queued scheduling (the returned task slot must not be completed yet) is allowed, otherwise false
	 * @param allocationTimeout timeout before the slot allocation times out
	 * @return A {@link SlotSharingManager.MultiTaskSlotLocality} which contains the allocated {@link SlotSharingManager.MultiTaskSlot}
	 * 		and its locality wrt the given location preferences
	 */
	private CompletableFuture<SlotSharingManager.MultiTaskSlotLocality> allocateMultiTaskSlot(
		AbstractID groupId,
		SlotSharingManager slotSharingManager,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		Time allocationTimeout) {

		List<SlotInfo> resolvedRootSlotsInfo = slotSharingManager.listResolvedRootSlotInfo(groupId);

		SlotSelectionStrategy.SlotInfoAndLocality bestResolvedRootSlotWithLocality =
			slotSelectionStrategy.selectBestSlotForProfile(resolvedRootSlotsInfo, slotProfile);

		SlotInfo bestResolvedRootSlotInfo = bestResolvedRootSlotWithLocality.getSlotInfo();

		final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = bestResolvedRootSlotInfo != null ?
			new SlotSharingManager.MultiTaskSlotLocality(
				Preconditions.checkNotNull(slotSharingManager.getResolvedRootSlot(bestResolvedRootSlotInfo)),
				bestResolvedRootSlotWithLocality.getLocality()) :
			null;

		if (multiTaskSlotLocality != null && multiTaskSlotLocality.getLocality() == Locality.LOCAL) {
			return CompletableFuture.completedFuture(multiTaskSlotLocality);
		}

		final SlotRequestId allocatedSlotRequestId = new SlotRequestId();
		final SlotRequestId multiTaskSlotRequestId = new SlotRequestId();

		CompletableFuture<SlotAndLocality> fromAvailableFuture =
			tryAllocateFromAvailable(allocatedSlotRequestId, slotProfile);

		return fromAvailableFuture.thenComposeAsync((SlotAndLocality poolSlotAndLocality) -> {

			if (poolSlotAndLocality != null &&
				(poolSlotAndLocality.getLocality() == Locality.LOCAL || bestResolvedRootSlotInfo == null)) {

				final AllocatedSlot allocatedSlot = poolSlotAndLocality.getSlot();
				final SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.createRootSlot(
					multiTaskSlotRequestId,
					CompletableFuture.completedFuture(poolSlotAndLocality.getSlot()),
					allocatedSlotRequestId,
					componentMainThreadExecutor);

				if (allocatedSlot.tryAssignPayload(multiTaskSlot)) {
					return CompletableFuture.completedFuture(
						SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, poolSlotAndLocality.getLocality()));
				} else {
					multiTaskSlot.release(new FlinkException("Could not assign payload to allocated slot " +
						allocatedSlot.getAllocationId() + '.')); // TODO
				}
			}

			if (multiTaskSlotLocality != null) {
				// prefer slot sharing group slots over unused slots
				if (poolSlotAndLocality != null) {
					return slotPoolGateway
						.releaseSlot(allocatedSlotRequestId, new FlinkException("Locality constraint is not better fulfilled by allocated slot."))
						.thenCompose((acknowledge -> CompletableFuture.completedFuture(multiTaskSlotLocality)));
				}
			}

			if (allowQueuedScheduling) {
				// there is no slot immediately available --> check first for uncompleted slots at the slot sharing group
				SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.getUnresolvedRootSlot(groupId);

				if (multiTaskSlot == null) {
					// it seems as if we have to request a new slot from the resource manager, this is always the last resort!!!
					final CompletableFuture<AllocatedSlot> slotAllocationFuture = slotPoolGateway.requestNewAllocatedSlot(
						allocatedSlotRequestId,
						slotProfile.getResourceProfile(),
						allocationTimeout);

					multiTaskSlot = slotSharingManager.createRootSlot(
						multiTaskSlotRequestId,
						slotAllocationFuture,
						allocatedSlotRequestId,
						componentMainThreadExecutor);

					slotAllocationFuture.whenCompleteAsync(
						(AllocatedSlot allocatedSlot, Throwable throwable) -> {
							final SlotSharingManager.TaskSlot taskSlot = slotSharingManager.getTaskSlot(multiTaskSlotRequestId);

							if (taskSlot != null) {
								// still valid
								if (!(taskSlot instanceof SlotSharingManager.MultiTaskSlot) || throwable != null) {
									taskSlot.release(throwable);
								} else {
									if (!allocatedSlot.tryAssignPayload(((SlotSharingManager.MultiTaskSlot) taskSlot))) {
										taskSlot.release(new FlinkException("Could not assign payload to allocated slot " +
											allocatedSlot.getAllocationId() + '.'));
									}
								}
							} else {
								slotPoolGateway.releaseSlot(
									allocatedSlotRequestId,
									new FlinkException("Could not find task slot with " + multiTaskSlotRequestId + '.'));
							}
						}, componentMainThreadExecutor);
				}

				return CompletableFuture.completedFuture(
					SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, Locality.UNKNOWN));
			}

			return FutureUtils.completedExceptionally(
				new NoResourceAvailableException("Could not allocate a shared slot for " + groupId + '.'));
		}, componentMainThreadExecutor);
	}

	private Acknowledge releaseSharedSlot(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {

		final SlotSharingManager multiTaskSlotManager = slotSharingManagersMap.get(slotSharingGroupId);

		if (multiTaskSlotManager != null) {
			final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(slotRequestId);

			if (taskSlot != null) {
				taskSlot.release(cause);
			} else {
				log.debug("Could not find slot [{}] in slot sharing group {}. Ignoring release slot request.", slotRequestId, slotSharingGroupId);
			}
		} else {
			log.debug("Could not find slot sharing group {}. Ignoring release slot request.", slotSharingGroupId);
		}
		return Acknowledge.get();
	}
}
