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

import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Implementation of {@link org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet.OrderedSetCache} based
 * on an AVL-Tree. We chose the implementation from fastutil over JDK for performance reasons.
 *
 * <p>Maintainer notes: We can consider the following potential performance improvements. First, we could introduce a
 * bulk-load method to OrderedSetCache to exploit the fact that adding from an OrderedSetStore is already happening in
 * sorted order, e.g. there are more efficient ways to construct search trees from sorted elements. Second, we could
 * replace the internal AVL-Tree with an extended variant of {@link HeapPriorityQueueSet} that is organized as a
 * Min-Max-Heap.
 *
 * @param <E> type of the contained elements.
 */
public class MinMaxPriorityQueueOrderedSetCache<E extends HeapPriorityQueueElement>
	implements CachingInternalPriorityQueueSet.OrderedSetCache<E> {

	/** The tree is used to store cached elements. */
	@Nonnull
	private final HeapMinMaxPriorityQueueSet<E> minMaxHeapSet;

	/** The element comparator. */
	@Nonnull
	private final PriorityComparator<E> priorityComparator;

	/** The maximum capacity of the cache. */
	@Nonnegative
	private final int capacity;

	/**
	 * Creates a new {@link MinMaxPriorityQueueOrderedSetCache} with the given capacity and element comparator. Capacity must be > 0.
	 * @param priorityComparator comparator for the cached elements.
	 * @param capacity the capacity of the cache. Must be > 0.
	 */
	public MinMaxPriorityQueueOrderedSetCache(@Nonnull PriorityComparator<E> priorityComparator, @Nonnegative int capacity) {
		Preconditions.checkArgument(capacity > 0, "Cache capacity must be greater than 0.");
		this.minMaxHeapSet = new HeapMinMaxPriorityQueueSet<>(priorityComparator, capacity);
		this.priorityComparator = priorityComparator;
		this.capacity = capacity;
	}

	@Override
	public void add(@Nonnull E element) {
		assert !isFull();
		minMaxHeapSet.add(element);
	}

	@Override
	public void remove(@Nonnull E element) {
		minMaxHeapSet.remove(element);
	}

	@Override
	public boolean isFull() {
		return minMaxHeapSet.size() == capacity;
	}

	@Override
	public boolean isEmpty() {
		return minMaxHeapSet.isEmpty();
	}

	@Override
	public boolean isInLowerBound(@Nonnull E toCheck) {
		return minMaxHeapSet.isEmpty() || priorityComparator.comparePriority(peekLast(), toCheck) >= 0;
	}

	@Nullable
	@Override
	public E removeFirst() {
		return minMaxHeapSet.pollFirst();
	}

	@Nullable
	@Override
	public E removeLast() {
		return minMaxHeapSet.pollLast();
	}

	@Nullable
	@Override
	public E peekFirst() {
		return minMaxHeapSet.peekFirst();
	}

	@Nullable
	@Override
	public E peekLast() {
		return minMaxHeapSet.peekLast();
	}

	@Nonnull
	@Override
	public CloseableIterator<E> orderedIterator() {
		return minMaxHeapSet.iterator();
	}
}
