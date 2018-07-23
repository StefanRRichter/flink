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

import java.util.HashMap;

/**
 * A heap-based min-max priority queue with set semantics, similar to {@link HeapPriorityQueueSet}.
 *
 * @see HeapPriorityQueueSet
 * @see HeapMinMaxPriorityQueue
 * @param <T> type of the contained elements.
 */
public class HeapMinMaxPriorityQueueSet<T extends HeapPriorityQueueElement> extends HeapMinMaxPriorityQueue<T> {

	/** Map for de-duplication of inserts to achieve set semantics. */
	@Nonnull
	private final HashMap<T, T> dedupMap;

	public HeapMinMaxPriorityQueueSet(@Nonnull PriorityComparator<T> priorityComparator, @Nonnegative int queueSize) {
		super(priorityComparator, queueSize);
		this.dedupMap = new HashMap<>(queueSize);
	}

	@Override
	@Nullable
	public T poll() {
		return removeFromDedupMap(super.poll());
	}

	@Override
	public boolean add(@Nonnull T element) {
		return dedupMap.putIfAbsent(element, element) == null && super.add(element);
	}

	@Override
	public boolean remove(@Nonnull T toRemove) {
		T storedElement = dedupMap.remove(toRemove);
		return storedElement != null && super.remove(storedElement);
	}

	@Override
	public T pollLast() {
		return removeFromDedupMap(super.pollLast());
	}

	private T removeFromDedupMap(@Nullable T toRemove) {
		return toRemove != null ? dedupMap.remove(toRemove) : null;
	}

	@Override
	public void clear() {
		super.clear();
		dedupMap.clear();
	}
}
