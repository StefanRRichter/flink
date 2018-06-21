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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

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
public class HeapOrderedSet<T extends HeapOrderedSetElement>
	extends HeapOrderedSetBase<T>
	implements OrderedSetState<T> {

	/**
	 * Function to extract the key from contained elements.
	 */
	private final KeyExtractorFunction<T> keyExtractor;

	/**
	 * This array contains one hash set per key-group. The sets are used for fast de-duplication and deletes of elements.
	 */
	private final HashMap<T, T>[] deduplicationMapsByKeyGroup;

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

		super(elementComparator, minimumCapacity);

		this.keyExtractor = keyExtractor;

		this.totalNumberOfKeyGroups = totalNumberOfKeyGroups;
		this.keyGroupRange = keyGroupRange;

		final int keyGroupsInLocalRange = keyGroupRange.getNumberOfKeyGroups();
		final int deduplicationSetSize = 1 + minimumCapacity / keyGroupsInLocalRange;
		this.deduplicationMapsByKeyGroup = new HashMap[keyGroupsInLocalRange];
		for (int i = 0; i < keyGroupsInLocalRange; ++i) {
			deduplicationMapsByKeyGroup[i] = new HashMap<>(deduplicationSetSize);
		}
	}

	@Override
	@Nullable
	public T poll() {
		final T toRemove = super.poll();
		if (toRemove != null) {
			return getDedupMapForElement(toRemove).remove(toRemove);
		} else {
			return null;
		}
	}

	@Override
	public boolean add(@Nonnull T element) {
		return getDedupMapForElement(element).putIfAbsent(element, element) == null && super.add(element);
	}

	@Override
	public boolean remove(@Nonnull T elementToRemove) {
		T storedElement = getDedupMapForElement(elementToRemove).remove(elementToRemove);
		return storedElement != null && super.remove(storedElement);
	}

	@Override
	public void clear() {
		super.clear();
		for (HashMap<?, ?> elementHashMap :
			deduplicationMapsByKeyGroup) {
			elementHashMap.clear();
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

	private HashMap<T, T> getDedupMapForKeyGroup(
		@Nonnegative int keyGroupIdx) {
		return deduplicationMapsByKeyGroup[globalKeyGroupToLocalIndex(keyGroupIdx)];
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
}
