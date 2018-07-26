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

import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nonnull;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.TreeSet;

/**
 * Simple implementation of {@link org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet.OrderedSetStore}
 * for tests.
 */
public class TestOrderedStore<T> implements CachingInternalPriorityQueueSet.OrderedSetStore<T> {

	private final TreeSet<T> treeSet;

	public TestOrderedStore(Comparator<T> comparator) {
		this.treeSet = new TreeSet<>(comparator);
	}

	@Override
	public void add(@Nonnull T element) {
		treeSet.add(element);
	}

	@Override
	public void remove(@Nonnull T element) {
		treeSet.remove(element);
	}

	@Override
	public int size() {
		return treeSet.size();
	}

	@Nonnull
	@Override
	public CloseableIterator<T> orderedIterator() {
		// "snapshot iterator", so that we don't suffer from concurrent modification exceptions in the test.
		return new CloseableIterator<T>() {

			T next = treeSet.isEmpty() ? null : treeSet.first();

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public T next() {
				T res = next;
				if (res == null) {
					throw new NoSuchElementException();
				}
				next = treeSet.higher(next);
				return res;
			}

			@Override
			public void close() {
				next = null;
			}
		};
//		return CloseableIterator.adapterForIterator(new TreeSet<>(treeSet).iterator());
	}
}
