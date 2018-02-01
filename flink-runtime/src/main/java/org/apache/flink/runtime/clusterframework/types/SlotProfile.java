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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class SlotProfile {

	@Nonnull
	private final ResourceProfile resourceProfile;
	@Nonnull
	private final Collection<TaskManagerLocation> preferredLocations;
	@Nonnull
	private final Collection<AllocationID> priorAllocations;

	public SlotProfile(
		@Nonnull ResourceProfile resourceProfile,
		@Nonnull Collection<TaskManagerLocation> preferredLocations,
		@Nonnull Collection<AllocationID> priorAllocations) {
		this.resourceProfile = resourceProfile;
		this.preferredLocations = preferredLocations;
		this.priorAllocations = priorAllocations;
	}

	@Nonnull
	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	@Nonnull
	public Collection<TaskManagerLocation> getPreferredLocations() {
		return preferredLocations;
	}

	@Nonnull
	public Collection<AllocationID> getPriorAllocations() {
		return priorAllocations;
	}

	public ProfileToSlotContextMatcher matcher() {
		if(priorAllocations.isEmpty()) {
			return new LocalityAwareRequirementsToSlotMatcher(preferredLocations);
		} else {
			return new PreviousAllocationProfileToSlotContextMatcher(priorAllocations);
		}
	}

	public interface ProfileToSlotContextMatcher {
		<I, O> O findMatchWithLocality(
			Stream<I> candidates,
			Function<I, SlotContext> contextExtractor,
			Predicate<I> additionalRequirementsFilter,
			BiFunction<I, Locality, O> resultProducer);
	}

	public static class PreviousAllocationProfileToSlotContextMatcher implements ProfileToSlotContextMatcher {

		private final HashSet<AllocationID> priorAllocations;

		PreviousAllocationProfileToSlotContextMatcher(Collection<AllocationID> priorAllocations) {
			this.priorAllocations = new HashSet<>(priorAllocations);
		}

		public <I, O> O findMatchWithLocality(
			Stream<I> candidates,
			Function<I, SlotContext> contextExtractor,
			Predicate<I> additionalRequirementsFilter,
			BiFunction<I, Locality, O> resultProducer) {

			Predicate<I> filterByAllocation =
				(candidate) -> priorAllocations.contains(contextExtractor.apply(candidate).getAllocationId());

			return candidates
				.filter(filterByAllocation.and(additionalRequirementsFilter))
				.findFirst()
				.map((result) -> resultProducer.apply(result, Locality.LOCAL))
				.orElse(null);
		}
	}

	public static class LocalityAwareRequirementsToSlotMatcher implements ProfileToSlotContextMatcher {

		HashSet<ResourceID> preferredResourceIDs;
		HashSet<String> preferredFQHostNames;

		LocalityAwareRequirementsToSlotMatcher(Collection<TaskManagerLocation> locationPreferences) {

			this.preferredResourceIDs = new HashSet<>(locationPreferences.size());
			this.preferredFQHostNames = new HashSet<>(locationPreferences.size());

			for (TaskManagerLocation locationPreference : locationPreferences) {
				this.preferredResourceIDs.add(locationPreference.getResourceID());
				this.preferredFQHostNames.add(locationPreference.getFQDNHostname());
			}
		}

		@Override
		public  <I, O> O findMatchWithLocality(
			Stream<I> candidates,
			Function<I, SlotContext> contextExtractor,
			Predicate<I> additionalRequirementsFilter,
			BiFunction<I, Locality, O> resultProducer) {

			if (preferredFQHostNames.isEmpty()) {
				return candidates
					.filter(additionalRequirementsFilter)
					.findFirst()
					.map((result) -> resultProducer.apply(result, Locality.UNCONSTRAINED))
					.orElse(null);
			}

			Iterator<I> iterator = candidates.iterator();

			I matchByHostName = null;
			I matchByAdditionalRequirements = null;

			while (iterator.hasNext()) {

				I candidate = iterator.next();
				SlotContext slotContext = contextExtractor.apply(candidate);

				if (preferredResourceIDs.contains(slotContext.getTaskManagerLocation().getResourceID())) {

					if (additionalRequirementsFilter.test(candidate)) {
						return resultProducer.apply(candidate, Locality.LOCAL);
					}
				} else if (matchByHostName == null
					&& preferredFQHostNames.contains(slotContext.getTaskManagerLocation().getFQDNHostname())) {

					if (additionalRequirementsFilter.test(candidate)) {
						matchByHostName = candidate;
					}
				} else if (matchByAdditionalRequirements == null
					&& additionalRequirementsFilter.test(candidate)) {

					matchByAdditionalRequirements = candidate;
				}
			}

			if (matchByHostName != null) {
				return resultProducer.apply(matchByHostName, Locality.HOST_LOCAL);
			} else if (matchByAdditionalRequirements != null) {
				return resultProducer.apply(matchByAdditionalRequirements, Locality.NON_LOCAL);
			} else {
				return null;
			}
		}
	}
}
