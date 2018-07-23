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
package org.apache.flink.runtime.state.heap;

import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.Random;
import java.util.TreeSet;

/**
 * Test base for instances of
 * {@link org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet.OrderedSetCache}.
 */
public abstract class OrderedSetCacheTestBase {

	@Test
	public void testOrderedSetCacheContract() {
		final Random random = new Random(0x42);
		final int capacity = 5000;
		final int keySpaceUpperBound = 100 * capacity;
		final TreeSet<HeapPQInteger> checkSet = new TreeSet<>(HeapPQInteger::compareTo);
		final CachingInternalPriorityQueueSet.OrderedSetCache<HeapPQInteger> testInstance = createInstance(capacity);

		Assert.assertTrue(testInstance.isEmpty());

		while (checkSet.size() < capacity) {
			Assert.assertEquals(checkSet.size() >= capacity, testInstance.isFull());
			if (!checkSet.isEmpty() && random.nextInt(10) == 0) {
				final HeapPQInteger toDelete = pickContainedRandomElement(checkSet, random);
				Assert.assertTrue(checkSet.remove(toDelete));
				testInstance.remove(toDelete);
			} else {
				final HeapPQInteger randomValue = new HeapPQInteger(random.nextInt(keySpaceUpperBound));
				checkSet.add(randomValue);
				testInstance.add(randomValue);
			}
			Assert.assertEquals(checkSet.isEmpty(), testInstance.isEmpty());

			Assert.assertEquals(checkSet.first(), testInstance.peekFirst());
			Assert.assertEquals(checkSet.last(), testInstance.peekLast());

			Assert.assertFalse(testInstance.isInLowerBound(new HeapPQInteger(checkSet.last().value +1)));
			Assert.assertTrue(testInstance.isInLowerBound(checkSet.last()));
		}

		Assert.assertTrue(testInstance.isFull());
		Assert.assertFalse(testInstance.isInLowerBound(new HeapPQInteger(checkSet.last().value +1)));
		Assert.assertTrue(testInstance.isInLowerBound(checkSet.last()));

		testInstance.remove(pickNotContainedRandomElement(checkSet, random, keySpaceUpperBound));
		Assert.assertTrue(testInstance.isFull());

		HeapPQInteger containedKey = pickContainedRandomElement(checkSet, random);

		Assert.assertTrue(checkSet.remove(containedKey));
		testInstance.remove(containedKey);

		Assert.assertFalse(testInstance.isFull());

		for (int i = 0; i < capacity; ++i) {
			if (random.nextInt(1) == 0) {
				Assert.assertEquals(checkSet.pollFirst(), testInstance.removeFirst());
			} else {
				Assert.assertEquals(checkSet.pollLast(), testInstance.removeLast());
			}
		}

		Assert.assertFalse(testInstance.isFull());
		Assert.assertTrue(testInstance.isEmpty());
	}

	private HeapPQInteger pickNotContainedRandomElement(TreeSet<HeapPQInteger> checkSet, Random random, int upperBound) {
		HeapPQInteger notContainedKey;
		do {
			notContainedKey = new HeapPQInteger(random.nextInt(upperBound));
		} while (checkSet.contains(notContainedKey));
		return notContainedKey;
	}

	private HeapPQInteger pickContainedRandomElement(TreeSet<HeapPQInteger> checkSet, Random random) {
		assert !checkSet.isEmpty();
		return checkSet.ceiling(new HeapPQInteger(1 + random.nextInt(checkSet.last().value)));
	}

	protected abstract CachingInternalPriorityQueueSet.OrderedSetCache<HeapPQInteger> createInstance(int capacity);

	class HeapPQInteger implements HeapPriorityQueueElement, Comparable<HeapPQInteger> {

		private final int value;
		private int idx;

		HeapPQInteger(int value) {
			this.value = value;
			this.idx = Integer.MIN_VALUE;
		}

		@Override
		public int getInternalIndex() {
			return idx;
		}

		@Override
		public void setInternalIndex(int newIndex) {
			this.idx = newIndex;
		}

		public int getValue() {
			return value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			HeapPQInteger that = (HeapPQInteger) o;
			return getValue() == that.getValue();
		}

		@Override
		public int hashCode() {

			return Objects.hash(getValue());
		}

		@Override
		public int compareTo(HeapPQInteger o) {
			return Integer.compare(value, o.value);
		}
	}
}
