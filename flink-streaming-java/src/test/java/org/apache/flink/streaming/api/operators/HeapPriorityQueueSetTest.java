/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests for {@link HeapPriorityQueueSet}.
 */
public class HeapPriorityQueueSetTest extends TestLogger {

	private static final KeyGroupRange KEY_GROUP_RANGE = new KeyGroupRange(0, 1);

	private static void insertRandomTimers(
		HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerPriorityQueue,
		int count) {
		insertRandomTimers(timerPriorityQueue, null, count);
	}

	private static void insertRandomTimers(
		@Nonnull HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerPriorityQueue,
		@Nullable Set<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet,
		int count) {

		ThreadLocalRandom localRandom = ThreadLocalRandom.current();

		for (int i = 0; i < count; ++i) {
			TimerHeapInternalTimer<Integer, VoidNamespace> timer =
				new TimerHeapInternalTimer<>(localRandom.nextLong(), i, VoidNamespace.INSTANCE);
			if (checkSet != null) {
				Preconditions.checkState(checkSet.add(timer));
			}
			Assert.assertTrue(timerPriorityQueue.add(timer));
		}
	}

	private static HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> newPriorityQueue(
		int initialCapacity) {

		final KeyExtractorFunction<TimerHeapInternalTimer<Integer, VoidNamespace>> keyExtractorFunction =
			TimerHeapInternalTimer.getKeyExtractorFunction();

		final Comparator<TimerHeapInternalTimer<Integer, VoidNamespace>> timerComparator =
			TimerHeapInternalTimer.getTimerComparator();

		return new HeapPriorityQueueSet<>(
			timerComparator,
			keyExtractorFunction,
			initialCapacity,
			KEY_GROUP_RANGE,
			KEY_GROUP_RANGE.getNumberOfKeyGroups());
	}

	@Test
	public void testPeekPollOrder() {
		final int initialCapacity = 4;
		final int testSize = 1000;
		HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerPriorityQueue =
			newPriorityQueue(initialCapacity);
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet = new HashSet<>(testSize);

		insertRandomTimers(timerPriorityQueue, checkSet, testSize);

		long lastTimestamp = Long.MIN_VALUE;
		int lastSize = timerPriorityQueue.size();
		Assert.assertEquals(testSize, lastSize);
		InternalTimer<Integer, VoidNamespace> timer;
		while ((timer = timerPriorityQueue.peek()) != null) {
			Assert.assertFalse(timerPriorityQueue.isEmpty());
			Assert.assertEquals(lastSize, timerPriorityQueue.size());
			Assert.assertEquals(timer, timerPriorityQueue.poll());
			Assert.assertTrue(checkSet.remove(timer));
			Assert.assertTrue(timer.getTimestamp() >= lastTimestamp);
			lastTimestamp = timer.getTimestamp();
			--lastSize;
		}

		Assert.assertTrue(timerPriorityQueue.isEmpty());
		Assert.assertEquals(0, timerPriorityQueue.size());
		Assert.assertEquals(0, checkSet.size());
	}

	@Test
	public void testStopInsertMixKeepsOrder() {

		HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerPriorityQueue = newPriorityQueue(3);

		final int testSize = 345;
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet = new HashSet<>(testSize);

		insertRandomTimers(timerPriorityQueue, checkSet, testSize);

		// check that the whole set is still in order
		while (!checkSet.isEmpty()) {

			Iterator<TimerHeapInternalTimer<Integer, VoidNamespace>> iterator = checkSet.iterator();
			TimerHeapInternalTimer<Integer, VoidNamespace> timer = iterator.next();
			iterator.remove();
			Assert.assertTrue(timerPriorityQueue.remove(timer));
			Assert.assertEquals(checkSet.size(), timerPriorityQueue.size());

			long lastTimestamp = Long.MIN_VALUE;

			while ((timer = timerPriorityQueue.poll()) != null) {
				Assert.assertTrue(timer.getTimestamp() >= lastTimestamp);
				lastTimestamp = timer.getTimestamp();
			}

			Assert.assertTrue(timerPriorityQueue.isEmpty());

			timerPriorityQueue.addAll(checkSet);
		}
	}

	@Test
	public void testPoll() {
		HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerPriorityQueue = newPriorityQueue(3);

		Assert.assertNull(timerPriorityQueue.poll());

		final int testSize = 345;
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet = new HashSet<>(testSize);
		insertRandomTimers(timerPriorityQueue, checkSet, testSize);

		long lastTimestamp = Long.MIN_VALUE;
		while (!timerPriorityQueue.isEmpty()) {
			TimerHeapInternalTimer<Integer, VoidNamespace> removed = timerPriorityQueue.poll();
			Assert.assertNotNull(removed);
			Assert.assertTrue(checkSet.remove(removed));
			Assert.assertTrue(removed.getTimestamp() >= lastTimestamp);
			lastTimestamp = removed.getTimestamp();
		}
		Assert.assertTrue(checkSet.isEmpty());

		Assert.assertNull(timerPriorityQueue.poll());
	}

	@Test
	public void testIsEmpty() {
		HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerPriorityQueue =
			newPriorityQueue(1);

		Assert.assertTrue(timerPriorityQueue.isEmpty());

		timerPriorityQueue.add(new TimerHeapInternalTimer<>(42L, 4711, VoidNamespace.INSTANCE));
		Assert.assertFalse(timerPriorityQueue.isEmpty());

		timerPriorityQueue.poll();
		Assert.assertTrue(timerPriorityQueue.isEmpty());
	}

	@Test
	public void testBulkAddRestoredTimers() {
		final int testSize = 10;
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerSet = new HashSet<>(testSize);
		for (int i = 0; i < testSize; ++i) {
			timerSet.add(new TimerHeapInternalTimer<>(i, i, VoidNamespace.INSTANCE));
		}

		List<TimerHeapInternalTimer<Integer, VoidNamespace>> twoTimesTimerSet = new ArrayList<>(timerSet.size() * 2);
		twoTimesTimerSet.addAll(timerSet);
		twoTimesTimerSet.addAll(timerSet);

		HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerPriorityQueue =
			newPriorityQueue(1);

		timerPriorityQueue.addAll(twoTimesTimerSet);
		timerPriorityQueue.addAll(twoTimesTimerSet);

		Assert.assertEquals(timerSet.size(), timerPriorityQueue.size());

		for (TimerHeapInternalTimer<Integer, VoidNamespace> timer : timerPriorityQueue) {
			Assert.assertTrue(timerSet.remove(timer));
		}

		Assert.assertTrue(timerSet.isEmpty());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testToArray() {

		final int testSize = 10;

		List<TimerHeapInternalTimer<Integer, VoidNamespace>[]> tests = new ArrayList<>(2);
		tests.add(new TimerHeapInternalTimer[0]);
		tests.add(new TimerHeapInternalTimer[testSize]);
		tests.add(new TimerHeapInternalTimer[testSize + 1]);

		for (TimerHeapInternalTimer<Integer, VoidNamespace>[] testArray : tests) {

			Arrays.fill(testArray, new TimerHeapInternalTimer<>(42L, 4711, VoidNamespace.INSTANCE));

			HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet = new HashSet<>(testSize);

			HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerPriorityQueue =
				newPriorityQueue(1);

			Assert.assertEquals(testArray.length, timerPriorityQueue.toArray(testArray).length);

			insertRandomTimers(timerPriorityQueue, checkSet, testSize);

			TimerHeapInternalTimer<Integer, VoidNamespace>[] toArray = timerPriorityQueue.toArray(testArray);

			Assert.assertEquals((testArray.length >= testSize), (testArray == toArray));

			int count = 0;
			for (TimerHeapInternalTimer<Integer, VoidNamespace> o : toArray) {
				if (o == null) {
					break;
				}
				Assert.assertTrue(checkSet.remove(o));
				++count;
			}

			Assert.assertEquals(timerPriorityQueue.size(), count);
			Assert.assertTrue(checkSet.isEmpty());
		}
	}

	@Test
	public void testIterator() {
		HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerPriorityQueue =
			newPriorityQueue(1);

		// test empty iterator
		Iterator<TimerHeapInternalTimer<Integer, VoidNamespace>> iterator = timerPriorityQueue.iterator();
		Assert.assertFalse(iterator.hasNext());
		try {
			iterator.next();
			Assert.fail();
		} catch (NoSuchElementException ignore) {
		}

		// iterate some data
		final int testSize = 10;
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet = new HashSet<>(testSize);
		insertRandomTimers(timerPriorityQueue, checkSet, testSize);
		iterator = timerPriorityQueue.iterator();
		while (iterator.hasNext()) {
			Assert.assertTrue(checkSet.remove(iterator.next()));
		}
		Assert.assertTrue(checkSet.isEmpty());

		// test remove is not supported
		try {
			iterator.remove();
			Assert.fail();
		} catch (UnsupportedOperationException ignore) {
		}
	}

	@Test
	public void testClear() {
		HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerPriorityQueue =
			newPriorityQueue(1);

		int count = 10;
		insertRandomTimers(timerPriorityQueue, count);
		Assert.assertEquals(count, timerPriorityQueue.size());
		timerPriorityQueue.clear();
		Assert.assertEquals(0, timerPriorityQueue.size());
	}

	@Test
	public void testScheduleTimer() {
		HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerPriorityQueue =
			newPriorityQueue(1);

		final long timestamp = 42L;
		final Integer key = 4711;

		TimerHeapInternalTimer<Integer, VoidNamespace> timer =
			new TimerHeapInternalTimer<>(timestamp, key, VoidNamespace.INSTANCE);
		Assert.assertTrue(timerPriorityQueue.add(timer));
		Assert.assertFalse(timerPriorityQueue.add(timer));
		Assert.assertEquals(1, timerPriorityQueue.size());
		timer = timerPriorityQueue.poll();
		Assert.assertNotNull(timer);
		Assert.assertEquals(timestamp, timer.getTimestamp());
		Assert.assertEquals(key, timer.getKey());
		Assert.assertEquals(VoidNamespace.INSTANCE, timer.getNamespace());
	}

	@Test
	public void testStopTimer() {
		HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerPriorityQueue =
			newPriorityQueue(1);

		final long timestamp = 42L;
		final Integer key = 4711;
		final TimerHeapInternalTimer<Integer, VoidNamespace> timer =
			new TimerHeapInternalTimer<>(timestamp, key, VoidNamespace.INSTANCE);
		Assert.assertFalse(timerPriorityQueue.remove(timer));
		Assert.assertTrue(timerPriorityQueue.add(timer));
		Assert.assertTrue(timerPriorityQueue.remove(timer));
		Assert.assertFalse(timerPriorityQueue.remove(timer));
		Assert.assertTrue(timerPriorityQueue.isEmpty());
	}
}
