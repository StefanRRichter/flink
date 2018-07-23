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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkState;


/**
 * This class implements a min-max heap, in a single array, where the min heap and the max heap are separated
 * into the odd and even level of the tree. This implementation is based on code from Guava's MinMaxPriorityQueue,
 * see
 * <a href="https://github.com/google/guava/blob/master/guava/src/com/google/common/collect/MinMaxPriorityQueue.java">.
 *
 * @param <T> type of the contained elements.
 */
public class HeapMinMaxPriorityQueue<T extends HeapPriorityQueueElement> extends AbstractHeapPriorityQueue<T> {

	/**
	 * The index of the head element in the array that represents the heap.
	 */
	private static final int QUEUE_HEAD_INDEX = 0;

	/** View as a Min-Heap on the underlying array. **/
	@Nonnull
	private final Heap minHeap;

	/** View as a Max-Heap on the underlying array. **/
	@Nonnull
	private final Heap maxHeap;

	public HeapMinMaxPriorityQueue(@Nonnull PriorityComparator<T> priorityComparator, @Nonnegative int queueSize) {
		super(queueSize);
		this.minHeap = new Heap(priorityComparator);
		this.maxHeap = new Heap((left, right) -> priorityComparator.comparePriority(right, left));
		this.minHeap.otherHeap = maxHeap;
		this.maxHeap.otherHeap = minHeap;
	}

	/** Returns the index of the max element. */
	protected int getMaxElementIndex() {
		switch (size) {
			case 1:
				return QUEUE_HEAD_INDEX; // The lone element in the queue is the maximum.
			case 2:
				return 1; // The lone element in the maxHeap is the maximum.
			default:
				// The max element must sit on the first level of the maxHeap. It is
				// actually the *lesser* of the two from the maxHeap's perspective.
				return (maxHeap.compareElements(1, 2) <= 0) ? 1 : 2;
		}
	}

	/**
	 * Removes and returns the least element of this queue, or returns {@code null} if the queue is
	 * empty.
	 */
	@Nullable
	public T pollFirst() {
		return poll();
	}

	/**
	 * Retrieves, but does not remove, the least element of this queue, or returns {@code null} if the
	 * queue is empty.
	 */
	@Nullable
	public T peekFirst() {
		return peek();
	}

	/**
	 * Removes and returns the greatest element of this queue, or returns {@code null} if the queue is
	 * empty.
	 */
	@Nullable
	public T pollLast() {
		return isEmpty() ? null : removeInternal(getMaxElementIndex());
	}

	/**
	 * Retrieves, but does not remove, the greatest element of this queue, or returns {@code null} if
	 * the queue is empty.
	 */
	@Nullable
	public T peekLast() {
		return isEmpty() ? null : elementData(getMaxElementIndex());
	}

	@Override
	protected void addInternal(@Nonnull T toAdd) {
		int insertIndex = size++;

		growIfNeeded();

		// Adds the element to the end of the heap and bubbles it up to the correct
		// position.
		heapForIndex(insertIndex).bubbleUp(insertIndex, toAdd);
	}

	/**
	 * Removes the element at position {@code index}.
	 */
	private void removeAt(int index) {
		size--;
		if (size == index) {
			queue[size] = null;
			return;
		}
		T actualLastElement = elementData(size);
		int lastElementAt = heapForIndex(size).swapWithConceptuallyLastElement(actualLastElement);
		if (lastElementAt == index) {
			// 'actualLastElement' is now at 'lastElementAt', and the element that was at 'lastElementAt'
			// is now at the end of queue. If that's the element we wanted to remove in the first place,
			// don't try to (incorrectly) trickle it. Instead, just delete it and we're done.
			queue[size] = null;
			return;
		}
		T toTrickle = elementData(size);
		queue[size] = null;
		fillHole(index, toTrickle);
	}

	private void fillHole(int index, T toTrickle) {
		Heap heap = heapForIndex(index);
		// We consider elementData(index) a "hole", and we want to fill it
		// with the last element of the heap, toTrickle.
		// Since the last element of the heap is from the bottom level, we
		// optimistically fill index position with elements from lower levels,
		// moving the hole down. In most cases this reduces the number of
		// comparisons with toTrickle, but in some cases we will need to bubble it
		// all the way up again.
		int vacated = heap.fillHoleAt(index);
		// Try to see if toTrickle can be bubbled up min levels.
		int bubbledTo = heap.bubbleUpAlternatingLevels(vacated, toTrickle);
		if (bubbledTo == vacated) {
			// Could not bubble toTrickle up min levels, try moving
			// it from min level to max level (or max to min level) and bubble up
			// there.
			heap.tryCrossOverAndBubbleUp(vacated, toTrickle);
		}
	}

	/** Removes and returns the value at {@code index}. */
	@Override
	protected T removeInternal(int index) {
		T value = elementData(index);
		removeAt(index);
		return value;
	}

	@Nonnull
	private Heap heapForIndex(int i) {
		return isEvenLevel(i) ? minHeap : maxHeap;
	}

	private static final int EVEN_POWERS_OF_TWO = 0x55555555;
	private static final int ODD_POWERS_OF_TWO = 0xaaaaaaaa;

	private static boolean isEvenLevel(int index) {
		int oneBased = ~~(index + 1);
		return (oneBased & EVEN_POWERS_OF_TWO) > (oneBased & ODD_POWERS_OF_TWO);
	}

	/**
	 * Each instance of MinMaxPriortyQueue encapsulates two instances of Heap: a min-heap and a
	 * max-heap. Conceptually, these might each have their own array for storage, but for efficiency's
	 * sake they are stored interleaved on alternate heap levels in the same array (MMPQ.queue).
	 */
	private class Heap {

		@Nonnull
		final PriorityComparator<T> priorityComparator;

		/** The min or max counterpart. */
		Heap otherHeap;

		Heap(@Nonnull PriorityComparator<T> priorityComparator) {
			this.priorityComparator = priorityComparator;
		}

		int compareElements(int a, int b) {
			return priorityComparator.comparePriority(elementData(a), elementData(b));
		}

		/**
		 * Tries to move {@code toTrickle} from a min to a max level and bubble up there. If it moved
		 * before {@code removeIndex} this method returns a pair as described in {@link #removeAt}.
		 */
		void tryCrossOverAndBubbleUp(int vacated, T toTrickle) {
			int crossOver = crossOver(vacated, toTrickle);
			if (crossOver == vacated) {
				return;
			}
			// bubble it up the opposite heap
			otherHeap.bubbleUpAlternatingLevels(crossOver, toTrickle);
		}

		/** Bubbles a value from {@code index} up the appropriate heap if required. */
		void bubbleUp(int index, T x) {
			int crossOver = crossOverUp(index, x);

			Heap heap;
			if (crossOver == index) {
				heap = this;
			} else {
				index = crossOver;
				heap = otherHeap;
			}
			heap.bubbleUpAlternatingLevels(index, x);
		}

		/**
		 * Bubbles a value from {@code index} up the levels of this heap, and returns the index the
		 * element ended up at.
		 */
		int bubbleUpAlternatingLevels(int index, T x) {
			while (index > 2) {
				int grandParentIndex = getGrandparentIndex(index);
				T e = elementData(grandParentIndex);
				if (priorityComparator.comparePriority(e, x) <= 0) {
					break;
				}
				moveElementToIdx(e, index);
				index = grandParentIndex;
			}
			moveElementToIdx(x, index);
			return index;
		}

		/**
		 * Returns the index of minimum value between {@code index} and {@code index + len}, or {@code
		 * -1} if {@code index} is greater than {@code size}.
		 */
		int findMin(int index, int len) {
			if (index >= size) {
				return -1;
			}
			checkState(index > QUEUE_HEAD_INDEX);
			int limit = Math.min(index, size - len) + len;
			int minIndex = index;
			for (int i = index + 1; i < limit; i++) {
				if (compareElements(i, minIndex) < 0) {
					minIndex = i;
				}
			}
			return minIndex;
		}

		/** Returns the minimum child or {@code -1} if no child exists. */
		int findMinChild(int index) {
			return findMin(getLeftChildIndex(index), 2);
		}

		/** Returns the minimum grand child or -1 if no grand child exists. */
		int findMinGrandChild(int index) {
			int leftChildIndex = getLeftChildIndex(index);
			if (leftChildIndex < QUEUE_HEAD_INDEX) {
				return -1;
			}
			return findMin(getLeftChildIndex(leftChildIndex), 4);
		}

		/**
		 * Moves an element one level up from a min level to a max level (or vice versa). Returns the
		 * new position of the element.
		 */
		int crossOverUp(int index, T x) {
			if (index == QUEUE_HEAD_INDEX) {
				moveElementToIdx(x, QUEUE_HEAD_INDEX);
				return QUEUE_HEAD_INDEX;
			}
			int parentIndex = getParentIndex(index);
			T parentElement = elementData(parentIndex);
			if (parentIndex != QUEUE_HEAD_INDEX) {
				// This is a guard for the case of the childless uncle.
				// Since the end of the array is actually the middle of the heap,
				// a smaller childless uncle can become a child of x when we
				// bubble up alternate levels, violating the invariant.
				int grandparentIndex = getParentIndex(parentIndex);
				int uncleIndex = getRightChildIndex(grandparentIndex);
				if (uncleIndex != parentIndex && getLeftChildIndex(uncleIndex) >= size) {
					T uncleElement = elementData(uncleIndex);
					if (priorityComparator.comparePriority(uncleElement, parentElement) < 0) {
						parentIndex = uncleIndex;
						parentElement = uncleElement;
					}
				}
			}
			if (priorityComparator.comparePriority(parentElement, x) < 0) {
				moveElementToIdx(parentElement, index);
				moveElementToIdx(x, parentIndex);
				return parentIndex;
			}
			moveElementToIdx(x, index);
			return index;
		}

		/**
		 * Swap {@code actualLastElement} with the conceptually correct last element of the heap.
		 * Returns the index that {@code actualLastElement} now resides in.
		 *
		 * <p>Since the last element of the array is actually in the middle of the sorted structure, a
		 * childless uncle node could be smaller, which would corrupt the invariant if this element
		 * becomes the new parent of the uncle. In that case, we first switch the last element with its
		 * uncle, before returning.
		 */
		int swapWithConceptuallyLastElement(T actualLastElement) {
			int parentIndex = getParentIndex(size);
			if (parentIndex != QUEUE_HEAD_INDEX) {
				int grandparentIndex = getParentIndex(parentIndex);
				int uncleIndex = getRightChildIndex(grandparentIndex);
				if (uncleIndex != parentIndex && getLeftChildIndex(uncleIndex) >= size) {
					T uncleElement = elementData(uncleIndex);
					if (priorityComparator.comparePriority(uncleElement, actualLastElement) < 0) {
						moveElementToIdx(actualLastElement, uncleIndex);
						moveElementToIdx(uncleElement, size);
						return uncleIndex;
					}
				}
			}
			return size;
		}

		/**
		 * Crosses an element over to the opposite heap by moving it one level down (or up if there are
		 * no elements below it).
		 *
		 * <p>Returns the new position of the element.
		 */
		int crossOver(int index, T x) {
			int minChildIndex = findMinChild(index);
			// TODO(kevinb): split the && into two if's and move crossOverUp so it's
			// only called when there's no child.
			if ((minChildIndex > QUEUE_HEAD_INDEX) && (priorityComparator.comparePriority(elementData(minChildIndex), x) < 0)) {
				moveElementToIdx(elementData(minChildIndex), index);
				moveElementToIdx(x, minChildIndex);
				return minChildIndex;
			}
			return crossOverUp(index, x);
		}

		/**
		 * Fills the hole at {@code index} by moving in the least of its grandchildren to this position,
		 * then recursively filling the new hole created.
		 *
		 * @return the position of the new hole (where the lowest grandchild moved from, that had no
		 *     grandchild to replace it)
		 */
		int fillHoleAt(int index) {
			int minGrandchildIndex;
			while ((minGrandchildIndex = findMinGrandChild(index)) > QUEUE_HEAD_INDEX) {
				moveElementToIdx(elementData(minGrandchildIndex), index);
				index = minGrandchildIndex;
			}
			return index;
		}

		// These would be static if inner classes could have static members.
		private int getLeftChildIndex(int i) {
			return i * 2 + 1;
		}

		private int getRightChildIndex(int i) {
			return i * 2 + 2;
		}

		private int getParentIndex(int i) {
			return (i - 1) / 2;
		}

		private int getGrandparentIndex(int i) {
			return getParentIndex(getParentIndex(i)); // (i - 3) / 4
		}
	}

	@Override
	protected int getHeadElementIndex() {
		return QUEUE_HEAD_INDEX;
	}

	private void growIfNeeded() {
		final int oldArraySize = queue.length;
		if (size > oldArraySize) {
			final int grow = (oldArraySize < 64) ? oldArraySize + 2 : oldArraySize >> 1;
			resizeQueueArray(oldArraySize + grow, oldArraySize + 1);
		}
	}

	@Nullable
	private T elementData(int index) {
		return queue[index];
	}
}
