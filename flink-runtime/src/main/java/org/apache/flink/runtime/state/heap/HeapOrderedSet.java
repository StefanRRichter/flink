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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.OrderedSetState;

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
import java.util.Set;

import static org.apache.flink.util.CollectionUtil.MAX_ARRAY_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A heap-based priority queue for {@link HeapOrderedSetElement} objects. This heap is supported by hash sets for fast
 * contains (de-duplication) and deletes. The heap implementation is a simple binary tree stored inside an array.
 * Element indexes in the heap array start at 1 instead of 0 to make array index computations a bit simpler in the hot
 * methods.
 *
 * <p>Possible future improvements:
 * <ul>
 *  <li>We could also implement shrinking for the heap and the deduplication maps.</li>
 *  <li>We could replace the deduplication maps with more efficient custom implementations. In particular, a hash set
 * would be enough if it could return existing elements on unsuccessful adding, etc..</li>
 * </ul>
 *
 * @param <T> type of the contained elements.
 */
public class HeapOrderedSet<T extends HeapOrderedSetElement> implements OrderedSetState<T> {

	/**
	 * Comparator for the contained elements.
	 */
	private final Comparator<T> elementComparator; //(o1, o2) -> Long.compare(o1.getTimestamp(), o2.getTimestamp());

	/**
	 * Function to extract the key from contained elements.
	 */
	private final KeyExtractorFunction<T> keyExtractor;

	/**
	 * This array contains one hash set per key-group. The sets are used for fast de-duplication and deletes of elements.
	 */
	private final HashMap<T, T>[] deduplicationMapsByKeyGroup;

	/**
	 * The array that represents the heap-organized priority queue.
	 */
	private T[] queue;

	/**
	 * The current size of the priority queue.
	 */
	private int size;

	/**
	 * The key-group range of elements that are managed by this queue.
	 */
	private final KeyGroupRange keyGroupRange;

	/**
	 * The total number of key-groups of the job.
	 */
	private final int totalNumberOfKeyGroups;

	/**
	 * Creates an empty {@link HeapOrderedSet} with the requested initial capacity.
	 *
	 * @param elementComparator comparator for the contained elements.
	 * @param keyExtractor function to extract a key from the contained elements.
	 * @param minimumCapacity the minimum and initial capacity of this priority queue.
	 * @param keyGroupRange the key-group range of the elements in this set.
	 * @param totalNumberOfKeyGroups the total number of key-groups of the job.
	 */
	@SuppressWarnings("unchecked")
	public HeapOrderedSet(
		@Nonnull Comparator<T> elementComparator,
		@Nonnull KeyExtractorFunction<T> keyExtractor,
		@Nonnegative int minimumCapacity,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int totalNumberOfKeyGroups) {

		this.elementComparator = elementComparator;
		this.keyExtractor = keyExtractor;

		this.totalNumberOfKeyGroups = totalNumberOfKeyGroups;
		this.keyGroupRange = keyGroupRange;

		final int keyGroupsInLocalRange = keyGroupRange.getNumberOfKeyGroups();
		final int deduplicationSetSize = 1 + minimumCapacity / keyGroupsInLocalRange;
		this.deduplicationMapsByKeyGroup = new HashMap[keyGroupsInLocalRange];
		for (int i = 0; i < keyGroupsInLocalRange; ++i) {
			deduplicationMapsByKeyGroup[i] = new HashMap<>(deduplicationSetSize);
		}

		this.queue = (T[]) new HeapOrderedSetElement[1 + minimumCapacity];
	}

	@Override
	@Nullable
	public T poll() {
		return size() > 0 ? removeElementAtIndex(1) : null;
	}

	@Override
	@Nullable
	public T peek() {
		return size() > 0 ? queue[1] : null;
	}

	@Override
	public boolean add(T toAdd) {
		return addInternal(toAdd);
	}

	@Override
	public boolean remove(T toStop) {
		return removeInternal(toStop);
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	@Nonnegative
	public int size() {
		return size;
	}

	@Override
	public void clear() {
		Arrays.fill(queue, null);
		for (HashMap<?, ?> elementHashMap :
			deduplicationMapsByKeyGroup) {
			elementHashMap.clear();
		}
		size = 0;
	}

	@SuppressWarnings({"unchecked"})
	@Nonnull
	public <O> O[] toArray(O[] out) {
		if (out.length < size) {
			return (O[]) Arrays.copyOfRange(queue, 1, size + 1, out.getClass());
		} else {
			System.arraycopy(queue, 1, out, 0, size);
			if (out.length > size) {
				out[size] = null;
			}
			return out;
		}
	}

	/**
	 * Returns an iterator over the elements in this queue. The iterator
	 * does not return the elements in any particular order.
	 *
	 * @return an iterator over the elements in this queue.
	 */
	@Nonnull
	public Iterator<T> iterator() {
		return new HeapIterator();
	}

	@Override
	public void addAll(@Nullable Collection<? extends T> restoredElements) {

		if (restoredElements == null) {
			return;
		}

		resizeForBulkLoad(restoredElements.size());

		for (T element : restoredElements) {
			addInternal(element);
		}
	}

	/**
	 * Returns an unmodifiable set of all elements in the given key-group.
	 */
	@Nonnull
	public Set<T> getElementsForKeyGroup(@Nonnegative int keyGroupIdx) {
		return Collections.unmodifiableSet(getDedupMapForKeyGroup(keyGroupIdx).keySet());
	}

	@VisibleForTesting
	@SuppressWarnings("unchecked")
	@Nonnull
	public List<Set<T>> getElementsByKeyGroup() {
		List<Set<T>> result = new ArrayList<>(deduplicationMapsByKeyGroup.length);
		for (int i = 0; i < deduplicationMapsByKeyGroup.length; ++i) {
			result.add(i, Collections.unmodifiableSet(deduplicationMapsByKeyGroup[i].keySet()));
		}
		return result;
	}

	private boolean addInternal(T element) {

		if (getDedupMapForElement(element).putIfAbsent(element, element) == null) {
			final int newSize = increaseSizeByOne();
			moveElementToIdx(element, newSize);
			siftUp(newSize);
			return true;
		} else {
			return false;
		}
	}

	private boolean removeInternal(T elementToRemove) {

		HeapOrderedSetElement storedElement = getDedupMapForElement(elementToRemove).remove(elementToRemove);

		if (storedElement != null) {
			removeElementAtIndex(storedElement.getManagedIndex());
			return true;
		}

		return false;
	}

	private T removeElementAtIndex(int removeIdx) {
		T[] heap = this.queue;
		T removedValue = heap[removeIdx];

		assert removedValue.getManagedIndex() == removeIdx;

		final int oldSize = size;

		if (removeIdx != oldSize) {
			T element = heap[oldSize];
			moveElementToIdx(element, removeIdx);
			siftDown(removeIdx);
			if (heap[removeIdx] == element) {
				siftUp(removeIdx);
			}
		}

		heap[oldSize] = null;
		getDedupMapForElement(removedValue).remove(removedValue);

		--size;
		return removedValue;
	}

	private void siftUp(int idx) {
		final T[] heap = this.queue;
		final T currentElement = heap[idx];
		int parentIdx = idx >>> 1;

		while (parentIdx > 0 && isElementLessThen(currentElement, heap[parentIdx])) {
			moveElementToIdx(heap[parentIdx], idx);
			idx = parentIdx;
			parentIdx >>>= 1;
		}

		moveElementToIdx(currentElement, idx);
	}

	private void siftDown(int idx) {
		final T[] heap = this.queue;
		final int heapSize = this.size;

		final T currentElement = heap[idx];
		int firstChildIdx = idx << 1;
		int secondChildIdx = firstChildIdx + 1;

		if (isElementIndexValid(secondChildIdx, heapSize) &&
			isElementLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
			firstChildIdx = secondChildIdx;
		}

		while (isElementIndexValid(firstChildIdx, heapSize) &&
			isElementLessThen(heap[firstChildIdx], currentElement)) {
			moveElementToIdx(heap[firstChildIdx], idx);
			idx = firstChildIdx;
			firstChildIdx = idx << 1;
			secondChildIdx = firstChildIdx + 1;

			if (isElementIndexValid(secondChildIdx, heapSize) &&
				isElementLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
				firstChildIdx = secondChildIdx;
			}
		}

		moveElementToIdx(currentElement, idx);
	}

	private HashMap<T, T> getDedupMapForKeyGroup(
		@Nonnegative int keyGroupIdx) {
		return deduplicationMapsByKeyGroup[globalKeyGroupToLocalIndex(keyGroupIdx)];
	}

	private boolean isElementIndexValid(int elementIndex, int heapSize) {
		return elementIndex <= heapSize;
	}

	private boolean isElementLessThen(T a, T b) {
		return elementComparator.compare(a, b) < 0;
	}

	private void moveElementToIdx(T element, int idx) {
		queue[idx] = element;
		element.setManagedIndex(idx);
	}

	private HashMap<T, T> getDedupMapForElement(T element) {
		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(
			keyExtractor.extractKeyFromElement(element),
			totalNumberOfKeyGroups);
		return getDedupMapForKeyGroup(keyGroup);
	}

	private int globalKeyGroupToLocalIndex(int keyGroup) {
		checkArgument(keyGroupRange.contains(keyGroup));
		return keyGroup - keyGroupRange.getStartKeyGroup();
	}

	private int increaseSizeByOne() {
		final int oldArraySize = queue.length;
		final int minRequiredNewSize = ++size;
		if (minRequiredNewSize >= oldArraySize) {
			final int grow = (oldArraySize < 64) ? oldArraySize + 2 : oldArraySize >> 1;
			resizeQueueArray(oldArraySize + grow, minRequiredNewSize);
		}
		// TODO implement shrinking as well?
		return minRequiredNewSize;
	}

	private void resizeForBulkLoad(int totalSize) {
		if (totalSize > queue.length) {
			int desiredSize = totalSize + (totalSize >>> 3);
			resizeQueueArray(desiredSize, totalSize);
		}
	}

	private void resizeQueueArray(int desiredSize, int minRequiredSize) {
		if (isValidArraySize(desiredSize)) {
			queue = Arrays.copyOf(queue, desiredSize);
		} else if (isValidArraySize(minRequiredSize)) {
			queue = Arrays.copyOf(queue, MAX_ARRAY_SIZE);
		} else {
			throw new OutOfMemoryError("Required minimum heap size " + minRequiredSize +
				" exceeds maximum size of " + MAX_ARRAY_SIZE + ".");
		}
	}

	private static boolean isValidArraySize(int size) {
		return size >= 0 && size <= MAX_ARRAY_SIZE;
	}

	/**
	 * {@link Iterator} implementation for {@link HeapOrderedSet}.
	 * {@link Iterator#remove()} is not supported.
	 */
	private class HeapIterator implements Iterator<T> {

		private int iterationIdx;

		HeapIterator() {
			this.iterationIdx = 0;
		}

		@Override
		public boolean hasNext() {
			return iterationIdx < size;
		}

		@Override
		public T next() {
			if (iterationIdx >= size) {
				throw new NoSuchElementException("Iterator has no next element.");
			}
			return queue[++iterationIdx];
		}
	}
}
