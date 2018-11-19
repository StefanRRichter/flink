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

import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.slotpool.PreviousAllocationSchedulingStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulingStrategy;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SlotProfileTest extends TestLogger {

	private final ResourceProfile resourceProfile = new ResourceProfile(2, 1024);

	private final AllocationID aid1 = new AllocationID();
	private final AllocationID aid2 = new AllocationID();
	private final AllocationID aid3 = new AllocationID();
	private final AllocationID aid4 = new AllocationID();
	private final AllocationID aidX = new AllocationID();

	private final TaskManagerLocation tml1 = new TaskManagerLocation(new ResourceID("tm-1"), InetAddress.getLoopbackAddress(), 42);
	private final TaskManagerLocation tml2 = new TaskManagerLocation(new ResourceID("tm-2"), InetAddress.getLoopbackAddress(), 43);
	private final TaskManagerLocation tml3 = new TaskManagerLocation(new ResourceID("tm-3"), InetAddress.getLoopbackAddress(), 44);
	private final TaskManagerLocation tml4 = new TaskManagerLocation(new ResourceID("tm-4"), InetAddress.getLoopbackAddress(), 45);
	private final TaskManagerLocation tmlX = new TaskManagerLocation(new ResourceID("tm-X"), InetAddress.getLoopbackAddress(), 46);

	private final TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

	private SimpleSlotContext ssc1 = new SimpleSlotContext(aid1, tml1, 1, taskManagerGateway);
	private SimpleSlotContext ssc2 = new SimpleSlotContext(aid2, tml2, 2, taskManagerGateway);
	private SimpleSlotContext ssc3 = new SimpleSlotContext(aid3, tml3, 3, taskManagerGateway);
	private SimpleSlotContext ssc4 = new SimpleSlotContext(aid4, tml4, 4, taskManagerGateway);

	private final Set<SlotContext> candidates = Collections.unmodifiableSet(createCandidates());

	private final SchedulingStrategy schedulingStrategy = PreviousAllocationSchedulingStrategy.getInstance();

	private Set<SlotContext> createCandidates() {
		Set<SlotContext> candidates = new HashSet<>(4);
		candidates.add(ssc1);
		candidates.add(ssc2);
		candidates.add(ssc3);
		candidates.add(ssc4);
		return candidates;
	}

	@Test
	public void matchNoRequirements() {

		SlotProfile slotProfile = new SlotProfile(resourceProfile, Collections.emptyList(), Collections.emptySet());
		SlotContext match = runMatching(slotProfile);

		Assert.assertTrue(candidates.contains(match));
	}

	@Test
	public void matchPreferredLocationNotAvailable() {

		SlotProfile slotProfile = new SlotProfile(resourceProfile, Collections.singletonList(tmlX), Collections.emptySet());
		SlotContext match = runMatching(slotProfile);

		Assert.assertTrue(candidates.contains(match));
	}

	@Test
	public void matchPreferredLocation() {

		SlotProfile slotProfile = new SlotProfile(resourceProfile, Collections.singletonList(tml2), Collections.emptySet());
		SlotContext match = runMatching(slotProfile);

		Assert.assertEquals(ssc2, match);

		slotProfile = new SlotProfile(resourceProfile, Arrays.asList(tmlX, tml4), Collections.emptySet());
		match = runMatching(slotProfile);

		Assert.assertEquals(ssc4, match);

		slotProfile = new SlotProfile(resourceProfile, Arrays.asList(tml3, tml1, tml3, tmlX), Collections.emptySet());
		match = runMatching(slotProfile);

		Assert.assertEquals(ssc3, match);
	}

	@Test
	public void matchPreviousAllocationOverridesPreferredLocation() {

		SlotProfile slotProfile = new SlotProfile(resourceProfile, Collections.singletonList(tml2), Collections.singleton(aid3));
		SlotContext match = runMatching(slotProfile);

		Assert.assertEquals(ssc3, match);

		slotProfile = new SlotProfile(resourceProfile, Arrays.asList(tmlX, tml1), new HashSet<>(Arrays.asList(aidX, aid2)));
		match = runMatching(slotProfile);

		Assert.assertEquals(ssc2, match);
	}

	@Test
	public void matchPreviousLocationNotAvailableButByLocality() {

		SlotProfile slotProfile = new SlotProfile(resourceProfile, Collections.singletonList(tml4), Collections.singleton(aidX));
		SlotContext match = runMatching(slotProfile);

		Assert.assertEquals(ssc4, match);
	}

	@Test
	public void matchPreviousLocationNotAvailableAndAllOthersBlacklisted() {
		HashSet<AllocationID> blacklisted = new HashSet<>(4);
		blacklisted.add(aid1);
		blacklisted.add(aid2);
		blacklisted.add(aid3);
		blacklisted.add(aid4);
		SlotProfile slotProfile = new SlotProfile(resourceProfile, Collections.singletonList(tml4), Collections.singleton(aidX), blacklisted);
		SlotContext match = runMatching(slotProfile);

		// there should be no valid option left and we expect null as return
		Assert.assertNull(match);
	}

	@Test
	public void matchPreviousLocationNotAvailableAndSomeOthersBlacklisted() {
		HashSet<AllocationID> blacklisted = new HashSet<>(3);
		blacklisted.add(aid1);
		blacklisted.add(aid3);
		blacklisted.add(aid4);
		SlotProfile slotProfile = new SlotProfile(resourceProfile, Collections.singletonList(tml4), Collections.singleton(aidX), blacklisted);
		SlotContext match = runMatching(slotProfile);

		// we expect that the candidate that is not blacklisted is returned
		Assert.assertEquals(ssc2, match);
	}

	@Test
	public void matchPreviousLocationAvailableButAlsoBlacklisted() {
		HashSet<AllocationID> blacklisted = new HashSet<>(4);
		blacklisted.add(aid1);
		blacklisted.add(aid2);
		blacklisted.add(aid3);
		blacklisted.add(aid4);
		SlotProfile slotProfile = new SlotProfile(resourceProfile, Collections.singletonList(tml3), Collections.singleton(aid3), blacklisted);
		SlotContext match = runMatching(slotProfile);

		// available previous allocation should override blacklisting
		Assert.assertEquals(ssc3, match);
	}

	private SlotContext runMatching(SlotProfile slotProfile) {
		return schedulingStrategy.findMatchWithLocality(
			slotProfile,
			candidates::stream,
			(candidate) -> candidate,
			(candidate) -> true,
			(candidate, locality) -> candidate);
	}
}
