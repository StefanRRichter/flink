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

import org.apache.flink.runtime.state.InternalPriorityQueueTestBase;
import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.util.CloseableIterator;

import java.util.TreeSet;

/**
 * Test for {@link CachingInternalPriorityQueueSet}.
 */
public class CachingInternalPriorityQueueSetTest extends InternalPriorityQueueTestBase {

	@Override
	protected InternalPriorityQueue<TestElement> newPriorityQueue(int initialCapacity) {
		final CachingInternalPriorityQueueSet.OrderedSetCache<TestElement> cache =
			new TreeOrderedSetCache<>(TEST_ELEMENT_COMPARATOR, 3);
		final CachingInternalPriorityQueueSet.OrderedSetStore<TestElement> store =
			new TestOrderedStore();
		return new CachingInternalPriorityQueueSet<>(cache, store);
	}

	@Override
	protected boolean testSetSemantics() {
		return true;
	}

	static final class TestOrderedStore implements CachingInternalPriorityQueueSet.OrderedSetStore<TestElement> {

		private final TreeSet<TestElement> treeSet;

		TestOrderedStore() {
			this.treeSet = new TreeSet<>(TEST_ELEMENT_COMPARATOR);
		}

		@Override
		public void add(TestElement element) {
			treeSet.add(element);
		}

		@Override
		public void remove(TestElement element) {
			treeSet.remove(element);
		}

		@Override
		public int size() {
			return treeSet.size();
		}

		@Override
		public CloseableIterator<TestElement> orderedIterator() {
			return CloseableIterator.adapterForIterator(treeSet.iterator());
		}
	}
}
