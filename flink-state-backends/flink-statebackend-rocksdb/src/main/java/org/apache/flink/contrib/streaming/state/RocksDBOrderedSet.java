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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.OrderedSetState;
import org.apache.flink.runtime.state.heap.HeapOrderedSetBase;
import org.apache.flink.runtime.state.heap.HeapOrderedSetElement;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

public class RocksDBOrderedSet<T> implements OrderedSetState<T> {

	private static final class SortedCacheComparator<T> implements Comparator<AbstractSortedFetchingCache<T>> {

		private final Comparator<T> elementComparator;

		SortedCacheComparator(Comparator<T> elementComparator) {
			this.elementComparator = elementComparator;
		}

		@Override
		public int compare(AbstractSortedFetchingCache<T> o1, AbstractSortedFetchingCache<T> o2) {
			final T leftTimer = o1.getFirst();
			final T rightTimer = o2.getFirst();

			if (leftTimer == null) {
				return (rightTimer == null ? 0 : 1);
			} else {
				return (rightTimer == null ? -1 : elementComparator.compare(leftTimer, rightTimer));
			}
		}
	}

	public interface SortedFetchingCacheFactory<T> {
		AbstractSortedFetchingCache<T> createCache(Comparator<T> elementComparator);
	}

	/**
	 * Function to extract the key from contained elements.
	 */
	private final KeyExtractorFunction<T> keyExtractor;
	private final AbstractSortedFetchingCache<T>[] keyGroupLists;
	private final int totalKeyGroups;
	private final int firstKeyGroup;

	@SuppressWarnings("unchecked")
	public RocksDBOrderedSet(
		KeyExtractorFunction<T> keyExtractor,
		Comparator<T> elementComparator,
		SortedFetchingCacheFactory<T> fetchingCacheFactory,
		KeyGroupRange keyGroupRange,
		int totalKeyGroups) {

		this.keyExtractor = keyExtractor;
		this.totalKeyGroups = totalKeyGroups;
		this.firstKeyGroup = keyGroupRange.getStartKeyGroup();
		this.keyGroupLists = new AbstractSortedFetchingCache[keyGroupRange.getNumberOfKeyGroups()];
		this.keyGroupHeap = new HeapOrderedSetBase<>(
			new SortedCacheComparator<>(elementComparator),
			keyGroupRange.getNumberOfKeyGroups());
		for (int i = 0; i < keyGroupLists.length; i++) {
			final AbstractSortedFetchingCache<T> keyGroupCache = fetchingCacheFactory.createCache(elementComparator);
			keyGroupLists[i] = keyGroupCache;
			keyGroupHeap.add(keyGroupCache);
		}
	}

	@Nonnull
	private final HeapOrderedSetBase<AbstractSortedFetchingCache<T>> keyGroupHeap;

	@Nullable
	@Override
	public T poll() {
		final AbstractSortedFetchingCache<T> headList = keyGroupHeap.peek();
		final T head = headList.removeFirst();
		keyGroupHeap.adjustElement(headList);
		keyGroupHeap.validate();
		return head;
	}

	@Nullable
	@Override
	public T peek() {
		return keyGroupHeap.peek().getFirst();
	}

	@Override
	public boolean add(@Nonnull T toAdd) {
		final AbstractSortedFetchingCache<T> list = getListForElementKeyGroup(toAdd);
		if (list.add(toAdd)) {
			keyGroupHeap.adjustElement(list);
			keyGroupHeap.validate();
			return list == keyGroupHeap.peek();
		} else {
			keyGroupHeap.validate();
			return false;
		}
	}

	@Override
	public boolean remove(@Nonnull T toRemove) {
		final AbstractSortedFetchingCache<T> list = getListForElementKeyGroup(toRemove);
		if (list.remove(toRemove)) {
			keyGroupHeap.adjustElement(list);
			keyGroupHeap.validate();
			return list == keyGroupHeap.peek();
		} else {
			keyGroupHeap.validate();
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
		for (AbstractSortedFetchingCache<T> list : keyGroupLists) {
			sizeSum += list.size();
		}
		return sizeSum;
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addAll(@Nullable Collection<? extends T> toAdd) {

		if (toAdd == null) {
			return;
		}

		for (T element : toAdd) {
			add(element);
		}
	}

	@Override
	public Iterator<T> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return "RocksDBOrderedSetStefan{" +
			"keyGroupLists=" + Arrays.toString(keyGroupLists) +
			'}';
	}

	private AbstractSortedFetchingCache<T> getListForElementKeyGroup(T element) {
		return keyGroupLists[computeKeyGroupIndex(element)];
	}

	private int computeKeyGroupIndex(T element) {
		return KeyGroupRangeAssignment.assignToKeyGroup(keyExtractor.extractKeyFromElement(element), totalKeyGroups) - firstKeyGroup;
	}

	public static abstract class AbstractSortedFetchingCache<E> implements HeapOrderedSetElement {

		protected final TreeSet<E> cache;
		protected final Comparator<E> comparator;
		protected final int capacity;

		private int pqManagedIndex;

		@SuppressWarnings("unchecked")
		public AbstractSortedFetchingCache(Comparator<E> comparator, int capacity) {
			this.comparator = comparator;
			this.capacity = capacity;
			this.cache = new TreeSet<>(comparator);
			this.pqManagedIndex = HeapOrderedSetElement.NOT_CONTAINED;
		}

		public E getFirst() {
			return !cache.isEmpty() ? cache.first() : null;
		}

		public E removeFirst() {

			final E first = cache.pollFirst();

			if (first != null) {

				removeFromBackend(first);

				if (cache.isEmpty()) {
					refillCacheFromBackend();
				}
			}

			checkConsistency();
			return first;
		}

		public boolean add(E toAdd) {
			if (cache.isEmpty() || comparator.compare(toAdd, cache.last()) < 0) {
				if (cache.add(toAdd) && cache.size() > capacity) {
					cache.pollLast();
				}
			}
			addToBackend(toAdd);
			checkConsistency();
			return cache.first() == toAdd;
		}

		public boolean remove(E toRemove) {

			boolean result = toRemove.equals(cache.first());

			cache.remove(toRemove);
			removeFromBackend(toRemove);
			if (cache.isEmpty()) {
				refillCacheFromBackend();
			}
			checkConsistency();
			return result;
		}

		protected void checkConsistency() {

		}

		@Override
		public int getManagedIndex() {
			return pqManagedIndex;
		}

		@Override
		public void setManagedIndex(int updateIndex) {
			this.pqManagedIndex = updateIndex;
		}

		protected abstract boolean addToCache(E element);

		protected abstract boolean removeFromCache(E element);

		protected abstract boolean isCacheFull();

		protected abstract boolean isCacheEmpty();

		protected abstract void addToBackend(E element);

		protected abstract void removeFromBackend(E element);

		protected abstract void refillCacheFromBackend();

		protected abstract int size();

		@Override
		public String toString() {
			return "AbstractSortedFetchingCache{" + cache + "}";
		}
	}
}
