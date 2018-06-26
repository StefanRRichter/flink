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

import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Comparator;

/**
 *
 * @param <T>
 */
public class PartitionedOrderedSet<T> implements InternalPriorityQueue<T> {

	private static final class SortedCacheComparator<T> implements Comparator<CachingInternalPriorityQueueSet<T>> {

		private final Comparator<T> elementComparator;

		SortedCacheComparator(Comparator<T> elementComparator) {
			this.elementComparator = elementComparator;
		}

		@Override
		public int compare(CachingInternalPriorityQueueSet<T> o1, CachingInternalPriorityQueueSet<T> o2) {
			final T leftTimer = o1.peek();
			final T rightTimer = o2.peek();

			if (leftTimer == null) {
				return (rightTimer == null ? 0 : 1);
			} else {
				return (rightTimer == null ? -1 : elementComparator.compare(leftTimer, rightTimer));
			}
		}
	}

	public interface CachingInternalPriorityQueueSetFactory<T> {
		CachingInternalPriorityQueueSet<T> createCache(int keyGroupId, Comparator<T> elementComparator);
	}

	/**
	 * Function to extract the key from contained elements.
	 */
	@Nonnull
	private final HeapPriorityQueue<CachingInternalPriorityQueueSet<T>> keyGroupHeap;
	@Nonnull
	private final KeyExtractorFunction<T> keyExtractor;
	@Nonnull
	private final CachingInternalPriorityQueueSet<T>[] keyGroupLists;

	private final int totalKeyGroups;
	private final int firstKeyGroup;

	@SuppressWarnings("unchecked")
	public PartitionedOrderedSet(
		@Nonnull KeyExtractorFunction<T> keyExtractor,
		@Nonnull Comparator<T> elementComparator,
		@Nonnull CachingInternalPriorityQueueSetFactory<T> orderedCacheFactory,
		@Nonnull KeyGroupRange keyGroupRange,
		int totalKeyGroups) {

		this.keyExtractor = keyExtractor;
		this.totalKeyGroups = totalKeyGroups;
		this.firstKeyGroup = keyGroupRange.getStartKeyGroup();
		this.keyGroupLists = new CachingInternalPriorityQueueSet[keyGroupRange.getNumberOfKeyGroups()];
		this.keyGroupHeap = new HeapPriorityQueue<>(
			new SortedCacheComparator<>(elementComparator),
			keyGroupRange.getNumberOfKeyGroups());
		for (int i = 0; i < keyGroupLists.length; i++) {
			final CachingInternalPriorityQueueSet<T> keyGroupCache =
				orderedCacheFactory.createCache(firstKeyGroup + i, elementComparator);
			keyGroupLists[i] = keyGroupCache;
			keyGroupHeap.add(keyGroupCache);
		}
	}

	@Nullable
	@Override
	public T poll() {
		final CachingInternalPriorityQueueSet<T> headList = keyGroupHeap.peek();
		final T head = headList.poll();
		keyGroupHeap.adjustElement(headList);
//		keyGroupHeap.validate();
		return head;
	}

	@Nullable
	@Override
	public T peek() {
		return keyGroupHeap.peek().peek();
	}

	@Override
	public boolean add(@Nonnull T toAdd) {
		final CachingInternalPriorityQueueSet<T> list = getListForElementKeyGroup(toAdd);
		if (list.add(toAdd)) {
			keyGroupHeap.adjustElement(list);
//			keyGroupHeap.validate();
			// do we have a new head?
			return list == keyGroupHeap.peek();
		} else {
//			keyGroupHeap.validate();
			// head unchanged
			return false;
		}
	}

	@Override
	public boolean remove(@Nonnull T toRemove) {
		final CachingInternalPriorityQueueSet<T> list = getListForElementKeyGroup(toRemove);
		if (list.remove(toRemove)) {
			keyGroupHeap.adjustElement(list);
//			keyGroupHeap.validate();
			// do we have a new head?
			return list == keyGroupHeap.peek();
		} else {
//			keyGroupHeap.validate();
			// head unchanged
			return false;
		}
	}

	@Override
	public boolean isEmpty() {
		return peek() == null;
	}

	@Override
	public int size() {
		int sizeSum = 0;
		for (CachingInternalPriorityQueueSet<T> list : keyGroupLists) {
			sizeSum += list.size();
		}
		return sizeSum;
	}

	@Override
	public void addAll(@Nullable Collection<? extends T> toAdd) {

		if (toAdd == null) {
			return;
		}

		// TODO we can bulk load the lists and then "heapify" after all elements have been inserted.
		for (T element : toAdd) {
			add(element);
		}
//		keyGroupHeap.validate();
	}

	@Override
	public CloseableIterator<T> iterator() {
		return new KeyGroupConcatenationIterator<>(keyGroupLists);
	}

	private CachingInternalPriorityQueueSet<T> getListForElementKeyGroup(T element) {
		return keyGroupLists[computeKeyGroupIndex(element)];
	}

	private int computeKeyGroupIndex(T element) {
		return KeyGroupRangeAssignment.assignToKeyGroup(keyExtractor.extractKeyFromElement(element), totalKeyGroups) - firstKeyGroup;
	}

	/**
	 *
	 * @param <T>
	 */
	private static final class KeyGroupConcatenationIterator<T> implements CloseableIterator<T> {

		@Nonnull
		private final CachingInternalPriorityQueueSet<T>[] keyGroupLists;

		@Nonnegative
		private int index;

		@Nonnull
		private CloseableIterator<T> current;

		private KeyGroupConcatenationIterator(@Nonnull CachingInternalPriorityQueueSet<T>[] keyGroupLists) {
			this.keyGroupLists = keyGroupLists;
			this.index = 0;
			this.current = CloseableIterator.empty();
		}

		@Override
		public boolean hasNext() {
			boolean currentHasNext = current.hasNext();
			while (!currentHasNext && index < keyGroupLists.length) {
				current = keyGroupLists[index++].iterator();
				currentHasNext = current.hasNext();
			}
			return currentHasNext;
		}

		@Override
		public T next() {
			return current.next();
		}

		@Override
		public void close() throws Exception {
			current.close();
		}
	}
}
