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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
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
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * TODO.
 */
public class Scheduler implements SlotProvider, SlotOwner {

	/**
	 * Calculates the candidate's locality score.
	 */
	private static final BiFunction<Integer, Integer, Integer> LOCALITY_EVALUATION_FUNCTION =
		(localWeigh, hostLocalWeigh) -> localWeigh * 10 + hostLocalWeigh;

	private static final CompletableFuture<LogicalSlot> CHAIN_ROOT = CompletableFuture.completedFuture(null);

	private final Logger log = LoggerFactory.getLogger(getClass());

	/** Managers for the different slot sharing groups. */
	@Nonnull
	private final Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagers;

	@Nonnull
	private final SlotPoolGateway slotPoolGateway;

	@Nonnull
	private CompletableFuture<?> allocationQueue;

	public Scheduler(
		@Nonnull Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagers,
		@Nonnull SlotPoolGateway slotPoolGateway) {
		this.slotSharingManagers = slotSharingManagers;
		this.slotPoolGateway = slotPoolGateway;
		this.allocationQueue = CHAIN_ROOT;
	}

	//---------------------------

	private <U> CompletableFuture<U> enqueueAllocation(Supplier<CompletionStage<U>> stageSupplier) {
		CompletableFuture<U> newChainTail = allocationQueue.thenCompose((ignored) -> stageSupplier.get());
		allocationQueue = newChainTail;
		return newChainTail;
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		boolean allowQueuedScheduling,
		SlotProfile slotProfile,
		Time allocationTimeout) {

		log.debug("Received slot request [{}] for task: {}", slotRequestId, scheduledUnit.getTaskToExecute());
		return enqueueAllocation(() ->
			slotPoolGateway.getAvailableSlotsInformation()
				.thenCompose((Iterator<SlotInfo> infoIterator) -> scheduledUnit.getSlotSharingGroupId() == null ?
					allocateSingleSlot(slotRequestId, slotProfile, infoIterator, allowQueuedScheduling, allocationTimeout) :
					allocateSharedSlot(slotRequestId, scheduledUnit, slotProfile, infoIterator, allowQueuedScheduling, allocationTimeout)));
	}

	@Override
	public CompletableFuture<Acknowledge> cancelSlotRequest(
		SlotRequestId slotRequestId,
		@Nullable SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {

		if (slotSharingGroupId != null) {
			return releaseSharedSlot(slotRequestId, slotSharingGroupId, cause);
		} else {
			return slotPoolGateway.releaseSlot(slotRequestId, cause);
		}
	}

	@Override
	public CompletableFuture<Boolean> returnAllocatedSlot(LogicalSlot logicalSlot) {

		SlotRequestId slotRequestId = logicalSlot.getSlotRequestId();
		SlotSharingGroupId slotSharingGroupId = logicalSlot.getSlotSharingGroupId();
		FlinkException cause = new FlinkException("Slot is being returned to the SlotPool.");

		CompletableFuture<Acknowledge> releaseSlotFuture = slotSharingGroupId != null ?
			releaseSharedSlot(slotRequestId, slotSharingGroupId, cause) :
			slotPoolGateway.releaseSlot(slotRequestId, cause);

		return releaseSlotFuture.thenApply((Acknowledge acknowledge) -> true);
	}

	//---------------------------

	private CompletableFuture<LogicalSlot> allocateSingleSlot(
		SlotRequestId slotRequestId,
		SlotProfile slotProfile,
		Iterator<SlotInfo> slotInfoList,
		boolean allowQueuedScheduling,
		Time allocationTimeout) {

		return scheduleSlotRequest(
				slotInfoList,
				slotProfile,
				allowQueuedScheduling,
				allocationTimeout)
			.thenCompose((SlotAndLocality slotAndLocality) -> {
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
					slotPoolGateway.releaseSlot(slotRequestId, flinkException); //TODO not though RPC or wait?
					throw new CompletionException(flinkException);
				}
			});
	}


	private CompletableFuture<SlotAndLocality> scheduleSlotRequest(
		@Nonnull Iterator<SlotInfo> slotInfoList,
		@Nonnull SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		@Nonnull Time allocationTimeout) {

		Tuple2<SlotInfo, Locality> bestSlotWithLocality = selectBestSlotForProfile(slotInfoList, slotProfile);
		SlotInfo bestSlot = bestSlotWithLocality.f0;

		final SlotRequestId slotRequestId = new SlotRequestId();

		if (bestSlot != null) {
			return slotPoolGateway
				.allocateAvailableSlot(slotRequestId, bestSlot.getAllocationId())
				.thenApply((AllocatedSlot allocatedSlot) -> new SlotAndLocality(allocatedSlot, bestSlotWithLocality.f1));

		} else if (allowQueuedScheduling) {
			return slotPoolGateway
				.requestNewAllocatedSlot(slotRequestId, slotProfile.getResourceProfile(), allocationTimeout)
				.thenApply((AllocatedSlot allocatedSlot) -> new SlotAndLocality(allocatedSlot, Locality.UNKNOWN));

		} else {
			return FutureUtils.completedExceptionally(
				new NoResourceAvailableException("Could not allocate a simple slot for " + slotRequestId + '.'));
		}
	}

	@Nonnull
	private Tuple2<SlotInfo, Locality> selectBestSlotForProfile(
		@Nonnull Iterator<SlotInfo> availableSlots,
		@Nonnull SlotProfile slotProfile) {

		Collection<TaskManagerLocation> locationPreferences = slotProfile.getPreferredLocations();

		// if we have no location preferences, we can only filter by the additional requirements.
		if (locationPreferences.isEmpty()) {
			if (availableSlots.hasNext()) {
				return Tuple2.of(availableSlots.next(), Locality.UNCONSTRAINED);
			} else {
				return Tuple2.of(null, Locality.UNKNOWN);
			}
		}

		// we build up two indexes, one for resource id and one for host names of the preferred locations.
		final Map<ResourceID, Integer> preferredResourceIDs = new HashMap<>(locationPreferences.size());
		final Map<String, Integer> preferredFQHostNames = new HashMap<>(locationPreferences.size());

		for (TaskManagerLocation locationPreference : locationPreferences) {
			preferredResourceIDs.merge(locationPreference.getResourceID(), 1, Integer::sum);
			preferredFQHostNames.merge(locationPreference.getFQDNHostname(), 1, Integer::sum);
		}

		final ResourceProfile resourceProfile = slotProfile.getResourceProfile();

		SlotInfo bestCandidate = null;
		Locality bestCandidateLocality = Locality.NON_LOCAL;
		int bestCandidateScore = Integer.MIN_VALUE;

		while (availableSlots.hasNext()) {
			SlotInfo candidate = availableSlots.next();
			if (candidate.getResourceProfile().isMatching(resourceProfile)) {

				// this gets candidate is local-weigh
				Integer localWeigh = preferredResourceIDs.getOrDefault(candidate.getTaskManagerLocation().getResourceID(), 0);

				// this gets candidate is host-local-weigh
				Integer hostLocalWeigh = preferredFQHostNames.getOrDefault(candidate.getTaskManagerLocation().getFQDNHostname(), 0);

				int candidateScore = LOCALITY_EVALUATION_FUNCTION.apply(localWeigh, hostLocalWeigh);
				if (candidateScore > bestCandidateScore) {
					bestCandidateScore = candidateScore;
					bestCandidate = candidate;
					bestCandidateLocality = localWeigh > 0 ? Locality.LOCAL : hostLocalWeigh > 0 ? Locality.HOST_LOCAL : Locality.NON_LOCAL;
				}
			}
		}

		// at the end of the iteration, we return the candidate with best possible locality or null.
		return Tuple2.of(bestCandidate, bestCandidateLocality);
	}

	// ------------------------------- slot sharing code

	private CompletableFuture<LogicalSlot> allocateSharedSlot(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		SlotProfile slotProfile,
		Iterator<SlotInfo> availableSlotsInfo,
		boolean allowQueuedScheduling,
		Time allocationTimeout) {

		// allocate slot with slot sharing
		final SlotSharingManager multiTaskSlotManager = slotSharingManagers.computeIfAbsent(
			scheduledUnit.getSlotSharingGroupId(),
			id -> new SlotSharingManager(
				id,
				slotPoolGateway, //TODO was "this", aka slot pool
				this)); //TODO!!!!!!

		final CompletableFuture<SlotSharingManager.MultiTaskSlotLocality> multiTaskSlotLocalityFuture;

		if (scheduledUnit.getCoLocationConstraint() != null) {
			multiTaskSlotLocalityFuture = allocateCoLocatedMultiTaskSlot(
				scheduledUnit.getCoLocationConstraint(),
				multiTaskSlotManager,
				slotProfile,
				availableSlotsInfo,
				allowQueuedScheduling,
				allocationTimeout);
		} else {
			multiTaskSlotLocalityFuture = allocateMultiTaskSlot(
				scheduledUnit.getJobVertexId(),
				multiTaskSlotManager,
				slotProfile,
				availableSlotsInfo,
				allowQueuedScheduling,
				allocationTimeout);
		}

		return multiTaskSlotLocalityFuture.thenCompose((SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality) -> {

				// sanity check
				Preconditions.checkState(!multiTaskSlotLocality.getMultiTaskSlot().contains(scheduledUnit.getJobVertexId()));

				final SlotSharingManager.SingleTaskSlot leaf = multiTaskSlotLocality.getMultiTaskSlot().allocateSingleTaskSlot(
					slotRequestId,
					scheduledUnit.getJobVertexId(),
					multiTaskSlotLocality.getLocality());

				return leaf.getLogicalSlotFuture();
			}
		);
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
	 * @throws NoResourceAvailableException if no task slot could be allocated
	 */
	private CompletableFuture<SlotSharingManager.MultiTaskSlotLocality> allocateCoLocatedMultiTaskSlot(
		CoLocationConstraint coLocationConstraint,
		SlotSharingManager multiTaskSlotManager,
		SlotProfile slotProfile,
		Iterator<SlotInfo> availableSlotsInfo,
		boolean allowQueuedScheduling,
		Time allocationTimeout) /* throws NoResourceAvailableException */ {
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
			availableSlotsInfo,
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
			final SlotSharingManager.MultiTaskSlot coLocationSlot = multiTaskSlotLocality.getMultiTaskSlot().allocateMultiTaskSlot(
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

			return CompletableFuture.completedFuture(SlotSharingManager.MultiTaskSlotLocality.of(coLocationSlot, multiTaskSlotLocality.getLocality()));
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
	 * @throws NoResourceAvailableException if no task slot could be allocated
	 */
	private CompletableFuture<SlotSharingManager.MultiTaskSlotLocality> allocateMultiTaskSlot(
		AbstractID groupId,
		SlotSharingManager slotSharingManager,
		SlotProfile slotProfile,
		Iterator<SlotInfo> poolSlotsInfo,
		boolean allowQueuedScheduling,
		Time allocationTimeout) /*throws NoResourceAvailableException*/ {

		Stream<SlotInfo> resolvedRootSlotsInfo = slotSharingManager.listResolvedRootSlotInfo(groupId);

		Tuple2<SlotInfo, Locality> bestResolvedRootSlotWithLocality =
			selectBestSlotForProfile(resolvedRootSlotsInfo.iterator(), slotProfile);

		SlotInfo bestResolvedRootSlotInfo = bestResolvedRootSlotWithLocality.f0;

		if (bestResolvedRootSlotInfo != null && bestResolvedRootSlotWithLocality.f1 == Locality.LOCAL) {
			return CompletableFuture.completedFuture(
				new SlotSharingManager.MultiTaskSlotLocality(
					Preconditions.checkNotNull(slotSharingManager.getResolvedRootSlot(bestResolvedRootSlotInfo)),
					Locality.LOCAL));
		}

		final SlotRequestId allocatedSlotRequestId = new SlotRequestId();
		final SlotRequestId multiTaskSlotRequestId = new SlotRequestId();

		Tuple2<SlotInfo, Locality> bestPoolSlotInfoWithLocality = selectBestSlotForProfile(poolSlotsInfo, slotProfile);
		SlotInfo poolSlotInfo = bestPoolSlotInfoWithLocality.f0;
		Locality poolSlotLocality = bestPoolSlotInfoWithLocality.f1;

		final CompletableFuture<SlotSharingManager.MultiTaskSlotLocality> fromPoolAttemptFuture = new CompletableFuture<>();

		if (poolSlotInfo != null && (poolSlotLocality == Locality.LOCAL || bestResolvedRootSlotInfo == null)) {

			CompletableFuture<AllocatedSlot> allocatedSlotFuture =
				slotPoolGateway.allocateAvailableSlot(allocatedSlotRequestId, poolSlotInfo.getAllocationId());

			final SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.createRootSlot(
				multiTaskSlotRequestId,
				allocatedSlotFuture,
				allocatedSlotRequestId);

			allocatedSlotFuture.thenAccept((AllocatedSlot allocatedSlot) -> {
				if (allocatedSlot.tryAssignPayload(multiTaskSlot)) {
					fromPoolAttemptFuture.complete(SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, poolSlotLocality));
				} else {
					multiTaskSlot.release(new FlinkException("Could not assign payload to allocated slot " +
						allocatedSlot.getAllocationId() + '.'));
					fromPoolAttemptFuture.complete(null);
				}
			});
		} else {
			fromPoolAttemptFuture.complete(null);
		}

		return fromPoolAttemptFuture.thenCompose((fromPool) -> { //TODO here must ensure we are running in main thread

			if (fromPool != null) {
				return CompletableFuture.completedFuture(fromPool);
			}

			if (bestResolvedRootSlotInfo != null) {
				return CompletableFuture.completedFuture(new SlotSharingManager.MultiTaskSlotLocality(
					Preconditions.checkNotNull(slotSharingManager.getResolvedRootSlot(bestResolvedRootSlotInfo)),
					bestResolvedRootSlotWithLocality.f1));
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
						allocatedSlotRequestId);

					slotAllocationFuture.whenComplete(
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
						});
				}

				return CompletableFuture.completedFuture(SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, Locality.UNKNOWN));
			}

			return FutureUtils.completedExceptionally(new NoResourceAvailableException("Could not allocate a shared slot for " + groupId + '.'));
		});
	}

	private CompletableFuture<Acknowledge> releaseSharedSlot(
		SlotRequestId slotRequestId,
		@Nonnull SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {

		final SlotSharingManager multiTaskSlotManager = slotSharingManagers.get(slotSharingGroupId);

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
		return CompletableFuture.completedFuture(Acknowledge.get());
	}
}
