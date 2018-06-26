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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Testbase for implementations of {@link InternalPriorityQueue}.
 */
public abstract class InternalPriorityQueueTestBase extends TestLogger {

	protected static final KeyGroupRange KEY_GROUP_RANGE = new KeyGroupRange(0, 1);
	protected static final KeyExtractorFunction<TestElement> KEY_EXTRACTOR_FUNCTION = TestElement::getKey;
	protected static final Comparator<TestElement> TEST_ELEMENT_COMPARATOR =
		Comparator.comparingLong(TestElement::getPriority).thenComparingLong(TestElement::getKey);

	protected static void insertRandomTimers(
		@Nonnull InternalPriorityQueue<TestElement> priorityQueueSet,
		@Nonnull Set<TestElement> checkSet,
		int count) {

		ThreadLocalRandom localRandom = ThreadLocalRandom.current();
//		Random localRandom = new Random(2);

		final int numUniqueKeys = Math.max(count / 4, 64);

		long duplicatePriority = Long.MIN_VALUE;

		for (int i = 0; i < count; ++i) {
			TestElement element;
			do {
				long elementPriority;
				if (duplicatePriority == Long.MIN_VALUE) {
					elementPriority = localRandom.nextLong();
				} else {
					elementPriority = duplicatePriority;
					duplicatePriority = Long.MIN_VALUE;
				}
				element = new TestElement(localRandom.nextInt(numUniqueKeys), elementPriority);
			} while (!checkSet.add(element));

			if (localRandom.nextInt(10) == 0) {
				duplicatePriority = element.getPriority();
			}
			priorityQueueSet.add(element);
		}
		Assert.assertEquals(count, priorityQueueSet.size());
	}

	@Test
	public void testPeekPollOrder() {
		final int initialCapacity = 4;
		final int testSize = 1000;
		InternalPriorityQueue<TestElement> priorityQueueSet =
			newPriorityQueue(initialCapacity);
		HashSet<TestElement> checkSet = new HashSet<>(testSize);

		insertRandomTimers(priorityQueueSet, checkSet, testSize);

		long lastPriorityValue = Long.MIN_VALUE;
		int lastSize = priorityQueueSet.size();
		Assert.assertEquals(testSize, lastSize);
		TestElement testElement;
		while ((testElement = priorityQueueSet.peek()) != null) {
			Assert.assertFalse(priorityQueueSet.isEmpty());
			Assert.assertEquals(lastSize, priorityQueueSet.size());
			Assert.assertEquals(testElement, priorityQueueSet.poll());
			Assert.assertTrue(checkSet.remove(testElement));
			Assert.assertTrue(testElement.getPriority() >= lastPriorityValue);
			lastPriorityValue = testElement.getPriority();
			--lastSize;
		}

		Assert.assertTrue(priorityQueueSet.isEmpty());
		Assert.assertEquals(0, priorityQueueSet.size());
		Assert.assertEquals(0, checkSet.size());
	}

	@Test
	public void testStopInsertMixKeepsOrder() {
		for(int i=0; i< 10; ++i) {
			InternalPriorityQueue<TestElement> priorityQueueSet = newPriorityQueue(3);

			final int testSize = 345;
			HashSet<TestElement> checkSet = new HashSet<>(testSize);

			insertRandomTimers(priorityQueueSet, checkSet, testSize);

			// check that the whole set is still in order
			while (!checkSet.isEmpty()) {

				Iterator<TestElement> iterator = checkSet.iterator();
				TestElement element = iterator.next();
				iterator.remove();
//			Assert.assertTrue(priorityQueueSet.remove(element)); //TODO check
				priorityQueueSet.remove(element);
				Assert.assertEquals(checkSet.size(), priorityQueueSet.size());

				long lastPriorityValue = Long.MIN_VALUE;

				while ((element = priorityQueueSet.poll()) != null) {
					Assert.assertTrue(element.getPriority() >= lastPriorityValue);
					lastPriorityValue = element.getPriority();
				}

				Assert.assertTrue(priorityQueueSet.isEmpty());

				priorityQueueSet.addAll(checkSet);
			}
		}
	}

	@Test
	public void testPoll() {
		InternalPriorityQueue<TestElement> priorityQueueSet = newPriorityQueue(3);

		Assert.assertNull(priorityQueueSet.poll());

		final int testSize = 345;
		HashSet<TestElement> checkSet = new HashSet<>(testSize);
		insertRandomTimers(priorityQueueSet, checkSet, testSize);

		long lastPriorityValue = Long.MIN_VALUE;
		while (!priorityQueueSet.isEmpty()) {
			TestElement removed = priorityQueueSet.poll();
			Assert.assertNotNull(removed);
			Assert.assertTrue(checkSet.remove(removed));
			Assert.assertTrue(removed.getPriority() >= lastPriorityValue);
			lastPriorityValue = removed.getPriority();
		}
		Assert.assertTrue(checkSet.isEmpty());

		Assert.assertNull(priorityQueueSet.poll());
	}

	@Test
	public void testIsEmpty() {
		InternalPriorityQueue<TestElement> priorityQueueSet =
			newPriorityQueue(1);

		Assert.assertTrue(priorityQueueSet.isEmpty());

		priorityQueueSet.add(new TestElement(4711L, 42L));
		Assert.assertFalse(priorityQueueSet.isEmpty());

		priorityQueueSet.poll();
		Assert.assertTrue(priorityQueueSet.isEmpty());
	}

	@Test
	public void testBulkAddRestoredTimers() throws Exception {
		final int testSize = 10;
		HashSet<TestElement> elementSet = new HashSet<>(testSize);
		for (int i = 0; i < testSize; ++i) {
			elementSet.add(new TestElement(i, i));
		}

		List<TestElement> twoTimesElementSet = new ArrayList<>(elementSet.size() * 2);

		for (TestElement testElement : elementSet) {
			twoTimesElementSet.add(testElement.deepCopy());
			twoTimesElementSet.add(testElement.deepCopy());
		}

		InternalPriorityQueue<TestElement> priorityQueueSet =
			newPriorityQueue(1);

		priorityQueueSet.addAll(twoTimesElementSet);
		priorityQueueSet.addAll(elementSet);

		final int expectedSize = testSetSemantics() ? elementSet.size() : 3 * elementSet.size();

		Assert.assertEquals(expectedSize, priorityQueueSet.size());
		try (final CloseableIterator<TestElement> iterator = priorityQueueSet.iterator()) {
			while (iterator.hasNext()) {
				if (testSetSemantics()) {
					Assert.assertTrue(elementSet.remove(iterator.next()));
				} else {
					Assert.assertTrue(elementSet.contains(iterator.next()));
				}
			}
		}
		Assert.assertTrue(elementSet.isEmpty());

	}

//	@Test
//	@SuppressWarnings("unchecked")
//	public void testToArray() {
//
//		final int testSize = 10;
//
//		List<TestElement[]> tests = new ArrayList<>(2);
//		tests.add(new TestElement[0]);
//		tests.add(new TestElement[testSize]);
//		tests.add(new TestElement[testSize + 1]);
//
//		for (TestElement[] testArray : tests) {
//
//			Arrays.fill(testArray, new TestElement<>(42L, 4711, VoidNamespace.INSTANCE));
//
//			HashSet<TestElement> checkSet = new HashSet<>(testSize);
//
//			InternalPriorityQueue<TestElement> timerPriorityQueue =
//				newPriorityQueue(1);
//
//			Assert.assertEquals(testArray.length, timerPriorityQueue.toArray(testArray).length);
//
//			insertRandomTimers(timerPriorityQueue, checkSet, testSize);
//
//			TestElement[] toArray = timerPriorityQueue.toArray(testArray);
//
//			Assert.assertEquals((testArray.length >= testSize), (testArray == toArray));
//
//			int count = 0;
//			for (TestElement o : toArray) {
//				if (o == null) {
//					break;
//				}
//				Assert.assertTrue(checkSet.remove(o));
//				++count;
//			}
//
//			Assert.assertEquals(timerPriorityQueue.size(), count);
//			Assert.assertTrue(checkSet.isEmpty());
//		}
//	}

	@Test
	public void testIterator() {
		InternalPriorityQueue<TestElement> priorityQueueSet =
			newPriorityQueue(1);

		// test empty iterator
		Iterator<TestElement> iterator = priorityQueueSet.iterator();
		Assert.assertFalse(iterator.hasNext());
		try {
			iterator.next();
			Assert.fail();
		} catch (NoSuchElementException ignore) {
		}

		// iterate some data
		final int testSize = 10;
		HashSet<TestElement> checkSet = new HashSet<>(testSize);
		insertRandomTimers(priorityQueueSet, checkSet, testSize);
		iterator = priorityQueueSet.iterator();
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
	public void testAdd() {
		InternalPriorityQueue<TestElement> priorityQueueSet =
			newPriorityQueue(1);

		final long key = 4711L;
		final long priorityValue = 42L;

		TestElement element = new TestElement(key, priorityValue);
		Assert.assertTrue(priorityQueueSet.add(element));
		if (testSetSemantics()) {
			priorityQueueSet.add(element.deepCopy());
		}
		Assert.assertEquals(1, priorityQueueSet.size());
		element = priorityQueueSet.poll();
		Assert.assertNotNull(element);
		Assert.assertEquals(priorityValue, element.getPriority());
		Assert.assertEquals(key, element.getKey());
	}

	@Test
	public void testRemove() {
		InternalPriorityQueue<TestElement> priorityQueueSet =
			newPriorityQueue(1);

		final long key = 4711L;
		final long priorityValue = 42L;
		final TestElement testElement = new TestElement(key, priorityValue);
		if (testSetSemantics()) {
			Assert.assertFalse(priorityQueueSet.remove(testElement));
		}
		Assert.assertTrue(priorityQueueSet.add(testElement));
		Assert.assertTrue(priorityQueueSet.remove(testElement));
		if (testSetSemantics()) {
			Assert.assertFalse(priorityQueueSet.remove(testElement));
		}
		Assert.assertTrue(priorityQueueSet.isEmpty());
	}

	protected abstract InternalPriorityQueue<TestElement> newPriorityQueue(int initialCapacity);

	protected abstract boolean testSetSemantics();

	/**
	 * Payload for usage in the test.
	 */
	protected static class TestElement implements HeapPriorityQueueElement {

		private final long key;
		private final long priority;
		private int internalIndex;

		public TestElement(long key, long priority) {
			this.key = key;
			this.priority = priority;
			this.internalIndex = NOT_CONTAINED;
		}

		@Override
		public int getInternalIndex() {
			return internalIndex;
		}

		@Override
		public void setInternalIndex(int newIndex) {
			internalIndex = newIndex;
		}

		public long getKey() {
			return key;
		}

		public long getPriority() {
			return priority;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			TestElement that = (TestElement) o;
			return getKey() == that.getKey() &&
				getPriority() == that.getPriority();
		}

		@Override
		public int hashCode() {
			return Objects.hash(getKey(), getPriority());
		}

		public TestElement deepCopy() {
			return new TestElement(key, priority);
		}
	}
}
