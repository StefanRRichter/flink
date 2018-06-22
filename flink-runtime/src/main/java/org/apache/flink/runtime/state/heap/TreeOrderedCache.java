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

import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;

import java.util.Comparator;

/**
 *
 * @param <E>
 */
public class TreeOrderedCache<E> implements CachingOrderedSetPartition.OrderedCache<E> {

	private final ObjectAVLTreeSet<E> avlTree;
	private final Comparator<E> elementComparator;
	private final int capacity;

	public TreeOrderedCache(Comparator<E> elementComparator, int capacity) {
		this.avlTree = new ObjectAVLTreeSet<>(elementComparator);
		this.elementComparator = elementComparator;
		this.capacity = capacity;
	}

	@Override
	public void add(E element) {
		assert !isFull();
		avlTree.add(element);
	}

	@Override
	public void remove(E element) {
		avlTree.remove(element);
	}

	@Override
	public boolean isFull() {
		return avlTree.size() == capacity;
	}

	@Override
	public boolean isEmpty() {
		return avlTree.isEmpty();
	}

	@Override
	public boolean isInLowerBound(E toCheck) {
		return avlTree.isEmpty() || elementComparator.compare(peekLast(), toCheck) > 0;
	}

	@Override
	public E removeFirst() {
		if (avlTree.isEmpty()) {
			return null;
		}
		final E first = avlTree.first();
		avlTree.remove(first);
		return first;
	}

	@Override
	public E removeLast() {
		if (avlTree.isEmpty()) {
			return null;
		}
		final E last = avlTree.last();
		avlTree.remove(last);
		return last;
	}

	@Override
	public E peekFirst() {
		return !avlTree.isEmpty() ? avlTree.first() : null;
	}

	@Override
	public E peekLast() {
		return !avlTree.isEmpty() ? avlTree.last() : null;
	}
}
