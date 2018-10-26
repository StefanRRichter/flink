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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Scheduler that assigns tasks to slots. This class is currently work in progress, comments will be updated as we
 * move forward.
 */
public class Scheduler implements SlotProvider, SlotOwner {

	/** Logger */
	private final Logger log = LoggerFactory.getLogger(getClass());

	/** Strategy that selects the best slot for a given slot allocation request. */
	@Nonnull
	private final SlotSelectionStrategy slotSelectionStrategy;

	/** The slot pool from which slots are allocated. */
	@Nonnull
	private final SlotPoolGateway slotPoolGateway;

	/** Executor for running tasks in the job master's main thread. */
	@Nonnull
	private Executor componentMainThreadExecutor;

	/** Managers for the different slot sharing groups. */
	@Nonnull
	private final Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagersMap;

	public Scheduler(
		@Nonnull Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagersMap,
		@Nonnull SlotSelectionStrategy slotSelectionStrategy,
		@Nonnull SlotPoolGateway slotPoolGateway) {

		this.slotSelectionStrategy = slotSelectionStrategy;
		this.slotSharingManagersMap = slotSharingManagersMap;
		this.slotPoolGateway = slotPoolGateway;
		this.componentMainThreadExecutor = (runnable) -> {
			throw new IllegalStateException("Main thread executor not initialized.");
		};
	}

	public void start(@Nonnull Executor mainThreadExecutor) {
		this.componentMainThreadExecutor = mainThreadExecutor;
	}

	//---------------------------

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		Time allocationTimeout) {
		log.debug("Received slot request [{}] for task: {}", slotRequestId, scheduledUnit.getTaskToExecute());

		final CompletableFuture<LogicalSlot> allocationResultFuture = new CompletableFuture<>();
		componentMainThreadExecutor.execute(() -> {

			CompletableFuture<LogicalSlot> allocationFuture = scheduledUnit.getSlotSharingGroupId() == null ?
				allocateSingleSlot(slotRequestId, slotProfile, allowQueuedScheduling, allocationTimeout) :
				allocateSharedSlot(slotRequestId, scheduledUnit, slotProfile, allowQueuedScheduling, allocationTimeout);

			allocationFuture.whenComplete((LogicalSlot slot, Throwable failure) -> {
				if (failure != null) {
					cancelSlotRequest(
						slotRequestId,
						scheduledUnit.getSlotSharingGroupId(),
						failure);
					allocationResultFuture.completeExceptionally(failure);
				} else {
					allocationResultFuture.complete(slot);
				}
			});
		});

		return allocationResultFuture;
	}

	@Override
	public CompletableFuture<Acknowledge> cancelSlotRequest(
		SlotRequestId slotRequestId,
		@Nullable SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {

		return CompletableFuture.supplyAsync(() -> {
			if (slotSharingGroupId != null) {
				releaseSharedSlot(slotRequestId, slotSharingGroupId, cause);
			} else {
				slotPoolGateway.releaseSlot(slotRequestId, cause);
			}
			return Acknowledge.get();
		}, componentMainThreadExecutor);
	}

	@Override
	public CompletableFuture<Boolean> returnAllocatedSlot(LogicalSlot logicalSlot) {

		SlotRequestId slotRequestId = logicalSlot.getSlotRequestId();
		SlotSharingGroupId slotSharingGroupId = logicalSlot.getSlotSharingGroupId();
		FlinkException cause = new FlinkException("Slot is being returned to the SlotPool.");

		return cancelSlotRequest(slotRequestId, slotSharingGroupId, cause).thenApply((ack) -> true);
	}

	//---------------------------

	private CompletableFuture<LogicalSlot> allocateSingleSlot(
		SlotRequestId slotRequestId,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		Time allocationTimeout) {

		Optional<SlotAndLocality> slotAndLocality =
			tryAllocateFromAvailable(slotRequestId, slotProfile);

		if (slotAndLocality.isPresent()) {
			// already successful from available
			try {
				return CompletableFuture.completedFuture(
					completeAllocationByAssigningPayload(slotRequestId, slotAndLocality.get()));
			} catch (FlinkException e) {
				return FutureUtils.completedExceptionally(e);
			}
		} else if (allowQueuedScheduling) {
			// we allocate by requesting a new slot
			return slotPoolGateway
				.requestNewAllocatedSlot(slotRequestId, slotProfile.getResourceProfile(), allocationTimeout)
				.thenApply((AllocatedSlotContext allocatedSlot) ->
				{
					try {
						return completeAllocationByAssigningPayload(slotRequestId, new SlotAndLocality(allocatedSlot, Locality.UNKNOWN));
					} catch (FlinkException e) {
						throw new CompletionException(e);
					}
				});
		} else {
			// failed to allocate
			return FutureUtils.completedExceptionally(
				new NoResourceAvailableException("Could not allocate a simple slot for " + slotRequestId + '.'));
		}
	}

	@Nonnull
	private LogicalSlot completeAllocationByAssigningPayload(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotAndLocality slotAndLocality) throws FlinkException {

		final AllocatedSlotContext allocatedSlot = slotAndLocality.getSlot();

		final SingleLogicalSlot singleTaskSlot = new SingleLogicalSlot(
			slotRequestId,
			allocatedSlot,
			null,
			slotAndLocality.getLocality(),
			this);

		if (allocatedSlot.tryAssignPayload(singleTaskSlot)) {
			return singleTaskSlot;
		} else {
			final FlinkException flinkException =
				new FlinkException("Could not assign payload to allocated slot " + allocatedSlot.getAllocationId() + '.');
			slotPoolGateway.releaseSlot(slotRequestId, flinkException);
			throw flinkException;
		}
	}

	private Optional<SlotAndLocality> tryAllocateFromAvailable(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotProfile slotProfile) {

		Collection<SlotInfo> slotInfoList = slotPoolGateway.getAvailableSlotsInformation();

		Optional<SlotSelectionStrategy.SlotInfoAndLocality> selectedAvailableSlot =
			slotSelectionStrategy.selectBestSlotForProfile(slotInfoList, slotProfile);


		if (selectedAvailableSlot.isPresent()) {
			SlotSelectionStrategy.SlotInfoAndLocality slotInfoAndLocality = selectedAvailableSlot.get();
			SlotInfo selectedSlotInfo = slotInfoAndLocality.getSlotInfo();

			Optional<AllocatedSlotContext> allocatedSlot =
				slotPoolGateway.allocateAvailableSlot(slotRequestId, selectedSlotInfo.getAllocationId());

			if (allocatedSlot.isPresent()) {
				return Optional.of(new SlotAndLocality(allocatedSlot.get(), slotInfoAndLocality.getLocality()));
			}
		}

		// no available slot fits
		return Optional.empty();
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
				slotPoolGateway,
				this));

		SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality;
		try {
			if (scheduledUnit.getCoLocationConstraint() != null) {
				multiTaskSlotLocality = allocateCoLocatedMultiTaskSlot(
					scheduledUnit.getCoLocationConstraint(),
					multiTaskSlotManager,
					slotProfile,
					allowQueuedScheduling,
					allocationTimeout);
			} else {
				multiTaskSlotLocality = allocateMultiTaskSlot(
					scheduledUnit.getJobVertexId(),
					multiTaskSlotManager,
					slotProfile,
					allowQueuedScheduling,
					allocationTimeout);
			}
		} catch (NoResourceAvailableException noResourceException) {
			return FutureUtils.completedExceptionally(noResourceException);
		}

		// sanity check
		Preconditions.checkState(!multiTaskSlotLocality.getMultiTaskSlot().contains(scheduledUnit.getJobVertexId()));

		final SlotSharingManager.SingleTaskSlot leaf = multiTaskSlotLocality.getMultiTaskSlot().allocateSingleTaskSlot(
			slotRequestId,
			scheduledUnit.getJobVertexId(),
			multiTaskSlotLocality.getLocality());
		return leaf.getLogicalSlotFuture();
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
	private SlotSharingManager.MultiTaskSlotLocality allocateCoLocatedMultiTaskSlot(
		CoLocationConstraint coLocationConstraint,
		SlotSharingManager multiTaskSlotManager,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		Time allocationTimeout) throws NoResourceAvailableException {
		final SlotRequestId coLocationSlotRequestId = coLocationConstraint.getSlotRequestId();

		if (coLocationSlotRequestId != null) {
			// we have a slot assigned --> try to retrieve it
			final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(coLocationSlotRequestId);

			if (taskSlot != null) {
				Preconditions.checkState(taskSlot instanceof SlotSharingManager.MultiTaskSlot);
				return SlotSharingManager.MultiTaskSlotLocality.of(((SlotSharingManager.MultiTaskSlot) taskSlot), Locality.LOCAL);
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
				slotProfile.getPreferredAllocations());
		}


		// get a new multi task slot
		SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = allocateMultiTaskSlot(
			coLocationConstraint.getGroupId(),
			multiTaskSlotManager,
			slotProfile,
			allowQueuedScheduling,
			allocationTimeout);

		// check whether we fulfill the co-location constraint
		if (coLocationConstraint.isAssigned() && multiTaskSlotLocality.getLocality() != Locality.LOCAL) {
			multiTaskSlotLocality.getMultiTaskSlot().release(
				new FlinkException("Multi task slot is not local and, thus, does not fulfill the co-location constraint."));

			throw new NoResourceAvailableException("Could not allocate a local multi task slot for the " +
				"co location constraint " + coLocationConstraint + '.');
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

		return SlotSharingManager.MultiTaskSlotLocality.of(coLocationSlot, multiTaskSlotLocality.getLocality());
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
	private SlotSharingManager.MultiTaskSlotLocality allocateMultiTaskSlot(
		AbstractID groupId,
		SlotSharingManager slotSharingManager,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		Time allocationTimeout) throws NoResourceAvailableException {

		Collection<SlotInfo> resolvedRootSlotsInfo = slotSharingManager.listResolvedRootSlotInfo(groupId);

		SlotSelectionStrategy.SlotInfoAndLocality bestResolvedRootSlotWithLocality =
			slotSelectionStrategy.selectBestSlotForProfile(resolvedRootSlotsInfo, slotProfile).orElse(null);

		final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = bestResolvedRootSlotWithLocality != null ?
			new SlotSharingManager.MultiTaskSlotLocality(
				slotSharingManager.getResolvedRootSlot(bestResolvedRootSlotWithLocality.getSlotInfo()),
				bestResolvedRootSlotWithLocality.getLocality()) :
			null;

		if (multiTaskSlotLocality != null && multiTaskSlotLocality.getLocality() == Locality.LOCAL) {
			return multiTaskSlotLocality;
		}

		final SlotRequestId allocatedSlotRequestId = new SlotRequestId();
		final SlotRequestId multiTaskSlotRequestId = new SlotRequestId();

		Optional<SlotAndLocality> optionalPoolSlotAndLocality = tryAllocateFromAvailable(allocatedSlotRequestId, slotProfile);

		if (optionalPoolSlotAndLocality.isPresent()) {
			SlotAndLocality poolSlotAndLocality = optionalPoolSlotAndLocality.get();
			if (poolSlotAndLocality.getLocality() == Locality.LOCAL || bestResolvedRootSlotWithLocality == null) {

				final AllocatedSlotContext allocatedSlot = poolSlotAndLocality.getSlot();
				final SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.createRootSlot(
					multiTaskSlotRequestId,
					CompletableFuture.completedFuture(poolSlotAndLocality.getSlot()),
					allocatedSlotRequestId);

				if (allocatedSlot.tryAssignPayload(multiTaskSlot)) {
					return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, poolSlotAndLocality.getLocality());
				} else {
					multiTaskSlot.release(new FlinkException("Could not assign payload to allocated slot " +
						allocatedSlot.getAllocationId() + '.'));
				}
			}
		}

		if (multiTaskSlotLocality != null) {
			// prefer slot sharing group slots over unused slots
			if (optionalPoolSlotAndLocality.isPresent()) {
				slotPoolGateway.releaseSlot(
					allocatedSlotRequestId,
					new FlinkException("Locality constraint is not better fulfilled by allocated slot."));
			}
			return multiTaskSlotLocality;
		}

		if (allowQueuedScheduling) {
			// there is no slot immediately available --> check first for uncompleted slots at the slot sharing group
			SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.getUnresolvedRootSlot(groupId);

			if (multiTaskSlot == null) {
				// it seems as if we have to request a new slot from the resource manager, this is always the last resort!!!
				final CompletableFuture<AllocatedSlotContext> slotAllocationFuture = slotPoolGateway.requestNewAllocatedSlot(
					allocatedSlotRequestId,
					slotProfile.getResourceProfile(),
					allocationTimeout);

				multiTaskSlot = slotSharingManager.createRootSlot(
					multiTaskSlotRequestId,
					slotAllocationFuture,
					allocatedSlotRequestId);

				slotAllocationFuture.whenCompleteAsync(
					(AllocatedSlotContext allocatedSlot, Throwable throwable) -> {
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

			return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, Locality.UNKNOWN);
		}

		throw new NoResourceAvailableException("Could not allocate a shared slot for " + groupId + '.');
	}

	private void releaseSharedSlot(
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
	}

	public boolean requiresPreviousExecutionGraphAllocations() {
		return slotSelectionStrategy instanceof PreviousAllocationSlotSelectionStrategy;
	}
}
