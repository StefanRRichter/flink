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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A heap-based priority queue for internal timers. This heap is supported by hash sets for fast contains
 * (de-duplication) and deletes. The heap implementation is a simple binary tree stored inside an array. Element indexes
 * in the heap array start at 1 instead of 0 to make array index computations a bit simpler in the hot methods.
 *
 * <p>Possible future improvements:
 * <ul>
 *  <li>We could also implement shrinking for the heap and the deduplication maps.</li>
 *  <li>We could replace the deduplication maps with more efficient custom implementations. In particular, a hash set
 * would be enough if it could return existing elements on unsuccessful adding, etc..</li>
 * </ul>
 *
 * @param <K> type of the key of the internal timers managed by this priority queue.
 * @param <N> type of the namespace of the internal timers managed by this priority queue.
 */
public class InternalTimerHeap<K, N> implements Queue<TimerHeapInternalTimer<K, N>>, Set<TimerHeapInternalTimer<K, N>> {

	/**
	 * A safe maximum size for arrays in the JVM.
	 */
	private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

	/**
	 * Comparator for {@link TimerHeapInternalTimer}, based on the timestamp in ascending order.
	 */
	private static final Comparator<TimerHeapInternalTimer<?, ?>> COMPARATOR =
		(o1, o2) -> Long.compare(o1.getTimestamp(), o2.getTimestamp());

	/**
	 * This array contains one hash set per key-group. The sets are used for fast de-duplication and deletes of timers.
	 */
	private final HashMap<TimerHeapInternalTimer<K, N>, TimerHeapInternalTimer<K, N>>[] deduplicationMapsByKeyGroup;

	/**
	 * The array that represents the heap-organized priority queue.
	 */
	private TimerHeapInternalTimer<K, N>[] queue;

	/**
	 * The current size of the priority queue.
	 */
	private int size;

	/**
	 * The key-group range of timers that are managed by this queue.
	 */
	private final KeyGroupRange keyGroupRange;

	/**
	 * The total number of key-groups of the job.
	 */
	private final int totalNumberOfKeyGroups;


	/**
	 * Creates an empty {@link InternalTimerHeap} with the requested initial capacity.
	 *
	 * @param minimumCapacity the minimum and initial capacity of this priority queue.
	 */
	@SuppressWarnings("unchecked")
	InternalTimerHeap(
		@Nonnegative int minimumCapacity,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int totalNumberOfKeyGroups) {

		this.totalNumberOfKeyGroups = totalNumberOfKeyGroups;
		this.keyGroupRange = keyGroupRange;

		final int keyGroupsInLocalRange = keyGroupRange.getNumberOfKeyGroups();
		final int deduplicationSetSize = 1 + minimumCapacity / keyGroupsInLocalRange;
		this.deduplicationMapsByKeyGroup = new HashMap[keyGroupsInLocalRange];
		for (int i = 0; i < keyGroupsInLocalRange; ++i) {
			deduplicationMapsByKeyGroup[i] = new HashMap<>(deduplicationSetSize);
		}

		this.queue = new TimerHeapInternalTimer[1 + minimumCapacity];
	}

	/**
	 * @see Set#add(Object)
	 */
	@Override
	public boolean add(@Nonnull TimerHeapInternalTimer<K, N> timer) {

		if (getDedupMapForKeyGroup(timer).putIfAbsent(timer, timer) == null) {
			final int newSize = ++this.size;
			checkCapacity(newSize);
			moveElementToIdx(timer, newSize);
			siftUp(newSize);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * This behaves like {@link #add(TimerHeapInternalTimer)}.
	 */
	@Override
	public boolean offer(@Nonnull TimerHeapInternalTimer<K, N> k) {
		return add(k);
	}

	@Nullable
	@Override
	public TimerHeapInternalTimer<K, N> poll() {
		return size() > 0 ? removeElementAtIndex(1) : null;
	}

	@Nonnull
	@Override
	public TimerHeapInternalTimer<K, N> remove() {
		TimerHeapInternalTimer<K, N> pollResult = poll();
		if (pollResult != null) {
			return pollResult;
		} else {
			throw new NoSuchElementException("InternalTimerPriorityQueue is empty.");
		}
	}

	@Nullable
	@Override
	public TimerHeapInternalTimer<K, N> peek() {
		return size() > 0 ? queue[1] : null;
	}

	@Nonnull
	@Override
	public TimerHeapInternalTimer<K, N> element() {
		TimerHeapInternalTimer<K, N> peekResult = peek();
		if (peekResult != null) {
			return peekResult;
		} else {
			throw new NoSuchElementException("InternalTimerPriorityQueue is empty.");
		}
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean contains(@Nullable Object o) {
		return (o instanceof TimerHeapInternalTimer)
			&& getDedupMapForKeyGroup((TimerHeapInternalTimer<?, ?>) o).containsKey(o);
	}

	@Override
	public boolean remove(@Nullable Object o) {
		if (o instanceof TimerHeapInternalTimer) {
			return removeInternal((TimerHeapInternalTimer<?, ?>) o);
		}
		return false;
	}

	@Override
	public boolean addAll(@Nullable Collection<? extends TimerHeapInternalTimer<K, N>> timers) {

		if (timers == null) {
			return true;
		}

		if (timers.size() > queue.length) {
			checkCapacity(timers.size());
		}

		for (TimerHeapInternalTimer<K, N> k : timers) {
			add(k);
		}

		return true;
	}

	@Nonnull
	@Override
	public Object[] toArray() {
		return Arrays.copyOfRange(queue, 1, size + 1);
	}

	@SuppressWarnings({"unchecked", "SuspiciousSystemArraycopy"})
	@Nonnull
	@Override
	public <T> T[] toArray(@Nonnull T[] array) {
		if (array.length >= size) {
			System.arraycopy(queue, 1, array, 0, size);
			return array;
		}
		return (T[]) Arrays.copyOfRange(queue, 1, size + 1, array.getClass());
	}

	@Override
	public boolean removeAll(@Nullable Collection<?> toRemove) {

		if (toRemove == null) {
			return false;
		}

		int oldSize = size();
		for (Object o : toRemove) {
			remove(o);
		}
		return size() == oldSize;
	}

	/**
	 * Returns an iterator over the elements in this queue. The iterator
	 * does not return the elements in any particular order.
	 *
	 * @return an iterator over the elements in this queue.
	 */
	@Nonnull
	@Override
	public Iterator<TimerHeapInternalTimer<K, N>> iterator() {
		return new InternalTimerPriorityQueueIterator();
	}

	@Override
	public boolean containsAll(@Nullable Collection<?> toCheck) {

		if (toCheck == null) {
			return true;
		}

		for (Object o : toCheck) {
			if (!contains(o)) {
				return false;
			}
		}

		return true;
	}

	@Nonnegative
	@Override
	public int size() {
		return size;
	}

	@Override
	public void clear() {

		Arrays.fill(queue, null);
		for (HashMap<TimerHeapInternalTimer<K, N>, TimerHeapInternalTimer<K, N>> timerHashMap :
			deduplicationMapsByKeyGroup) {
			timerHashMap.clear();
		}
		size = 0;
	}

	/**
	 * This method is currently not implemented.
	 */
	@Override
	public boolean retainAll(@Nullable Collection<?> toRetain) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Adds a new timer with the given timestamp, key, and namespace to the heap, if an identical timer was not yet
	 * registered.
	 *
	 * @param timestamp the timer timestamp.
	 * @param key the timer key.
	 * @param namespace the timer namespace.
	 * @return true iff a new timer with given timestamp, key, and namespace was added to the heap.
	 */
	boolean scheduleTimer(long timestamp, K key, N namespace) {
		return add(new TimerHeapInternalTimer<>(timestamp, key, namespace));
	}

	/**
	 * Stops timer with the given timestamp, key, and namespace by removing it from the heap, if it exists on the heap.
	 *
	 * @param timestamp the timer timestamp.
	 * @param key the timer key.
	 * @param namespace the timer namespace.
	 * @return true iff a timer with given timestamp, key, and namespace was found and removed from the heap.
	 */
	boolean stopTimer(long timestamp, K key, N namespace) {
		return removeInternal(new TimerHeapInternalTimer<>(timestamp, key, namespace));
	}

	/**
	 * This method adds all the given timers to the heap.
	 */
	void restoreTimers(Collection<? extends InternalTimer<K, N>> toAdd) {
		if (toAdd == null) {
			return;
		}

		if (toAdd.size() > queue.length) {
			checkCapacity(toAdd.size());
		}

		for (InternalTimer<K, N> k : toAdd) {
			if (k instanceof TimerHeapInternalTimer) {
				add((TimerHeapInternalTimer<K, N>) k);
			} else {
				scheduleTimer(k.getTimestamp(), k.getKey(), k.getNamespace());
			}
		}
	}

	private boolean removeInternal(TimerHeapInternalTimer<?, ?> timerToRemove) {

		TimerHeapInternalTimer<K, N> storedTimer = getDedupMapForKeyGroup(timerToRemove).remove(timerToRemove);

		if (storedTimer != null) {
			removeElementAtIndex(storedTimer.getTimerHeapIndex());
			return true;
		}

		return false;
	}

	private TimerHeapInternalTimer<K, N> removeElementAtIndex(int removeIdx) {
		TimerHeapInternalTimer<K, N>[] heap = this.queue;
		TimerHeapInternalTimer<K, N> removedValue = heap[removeIdx];

		assert removedValue.getTimerHeapIndex() == removeIdx;

		final int oldSize = size;

		if (removeIdx != oldSize) {
			TimerHeapInternalTimer<K, N> timer = heap[oldSize];
			moveElementToIdx(timer, removeIdx);
			siftDown(removeIdx);
			if (heap[removeIdx] == timer) {
				siftUp(removeIdx);
			}
		}

		heap[oldSize] = null;
		getDedupMapForKeyGroup(removedValue).remove(removedValue);

		--size;
		return removedValue;
	}

	private void siftUp(int idx) {
		final TimerHeapInternalTimer<K, N>[] heap = this.queue;
		TimerHeapInternalTimer<K, N> currentTimer = heap[idx];
		int parentIdx = idx >>> 1;

		while (parentIdx > 0 && COMPARATOR.compare(currentTimer, heap[parentIdx]) < 0) {
			moveElementToIdx(heap[parentIdx], idx);
			idx = parentIdx;
			parentIdx = parentIdx >>> 1;
		}

		moveElementToIdx(currentTimer, idx);
	}

	private void siftDown(int idx) {
		final TimerHeapInternalTimer<K, N>[] heap = this.queue;
		final int heapSize = this.size;

		TimerHeapInternalTimer<K, N> currentTimer = heap[idx];
		int firstChildIdx = idx << 1;
		int secondChildIdx = firstChildIdx + 1;

		if (secondChildIdx <= heapSize && COMPARATOR.compare(heap[secondChildIdx], heap[firstChildIdx]) < 0) {
			firstChildIdx = secondChildIdx;
		}

		while (firstChildIdx <= heapSize && COMPARATOR.compare(heap[firstChildIdx], currentTimer) < 0) {
			moveElementToIdx(heap[firstChildIdx], idx);
			idx = firstChildIdx;
			firstChildIdx = idx << 1;
			secondChildIdx = firstChildIdx + 1;

			if (secondChildIdx <= heapSize && COMPARATOR.compare(heap[secondChildIdx], heap[firstChildIdx]) < 0) {
				firstChildIdx = secondChildIdx;
			}
		}

		moveElementToIdx(currentTimer, idx);
	}

	/**
	 * Returns an unmodifiable set of all timers in the given key-group.
	 */
	Set<InternalTimer<K, N>> getTimersForKeyGroup(@Nonnegative int keyGroupIdx) {
		return Collections.unmodifiableSet(getDedupMapForKeyGroup(keyGroupIdx).keySet());
	}

	private HashMap<TimerHeapInternalTimer<K, N>, TimerHeapInternalTimer<K, N>> getDedupMapForKeyGroup(
		@Nonnegative int keyGroupIdx) {
		return deduplicationMapsByKeyGroup[globalKeyGroupToLocalIndex(keyGroupIdx)];
	}

	@VisibleForTesting
	@SuppressWarnings("unchecked")
	List<Set<InternalTimer<K, N>>> getTimersByKeyGroup() {
		List<Set<InternalTimer<K, N>>> result = new ArrayList<>(deduplicationMapsByKeyGroup.length);
		for (int i = 0; i < deduplicationMapsByKeyGroup.length; ++i) {
			result.add(i, Collections.unmodifiableSet(deduplicationMapsByKeyGroup[i].keySet()));
		}
		return result;
	}

	private void moveElementToIdx(TimerHeapInternalTimer<K, N> element, int idx) {
		queue[idx] = element;
		element.setTimerHeapIndex(idx);
	}

	private HashMap<TimerHeapInternalTimer<K, N>, TimerHeapInternalTimer<K, N>> getDedupMapForKeyGroup(
		TimerHeapInternalTimer<?, ?> timer) {
		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(timer.getKey(), totalNumberOfKeyGroups);
		return getDedupMapForKeyGroup(keyGroup);
	}

	private int globalKeyGroupToLocalIndex(int keyGroup) {
		checkArgument(keyGroupRange.contains(keyGroup));
		return keyGroup - keyGroupRange.getStartKeyGroup();
	}

	private void checkCapacity(int requested) {
		int oldArraySize = queue.length;

		if (requested >= oldArraySize) {
			final int grow = (oldArraySize < 64) ? oldArraySize + 2 : oldArraySize >> 1;
			int newArraySize = oldArraySize + grow;
			if (newArraySize - MAX_ARRAY_SIZE > 0) {
				if (newArraySize < 0 || requested > MAX_ARRAY_SIZE) {
					throw new OutOfMemoryError("Required timer heap exceeds maximum size!");
				} else {
					newArraySize = MAX_ARRAY_SIZE;
				}
			}
			queue = Arrays.copyOf(queue, newArraySize);
		}
		// TODO implement shrinking as well?
	}

	/**
	 * {@link Iterator} implementation for {@link InternalTimerPriorityQueueIterator}.
	 * {@link Iterator#remove()} is not supported.
	 */
	private class InternalTimerPriorityQueueIterator implements Iterator<TimerHeapInternalTimer<K, N>> {

		private int iterationIdx;

		InternalTimerPriorityQueueIterator() {
			this.iterationIdx = 0;
		}

		@Override
		public boolean hasNext() {
			return iterationIdx < size;
		}

		@Override
		public TimerHeapInternalTimer<K, N> next() {
			if (iterationIdx >= size) {
				throw new NoSuchElementException("Iterator has no next element.");
			}
			return queue[++iterationIdx];
		}
	}
}
