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

import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.InternalPriorityQueueTestBase;
import org.apache.flink.util.CloseableIterator;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

@RunWith(Parameterized.class)
public class HeapMinMaxPriorityQueueAndSetTest extends InternalPriorityQueueTestBase {

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{(Function<Integer, InternalPriorityQueue<TestElement>>) HeapMinMaxPriorityQueueAndSetTest::createQueue,
				(Comparator<Long>) Long::compareTo,
				false},
			{(Function<Integer, InternalPriorityQueue<TestElement>>) initCapacity ->
				new InverseTestHeapMinMaxPriorityQueue<>(createQueue(initCapacity)),
				((Comparator<Long>) Long::compareTo).reversed(),
				false},
			{(Function<Integer, InternalPriorityQueue<TestElement>>) HeapMinMaxPriorityQueueAndSetTest::createQueueSet,
				(Comparator<Long>) Long::compareTo,
				true},
			{(Function<Integer, InternalPriorityQueue<TestElement>>) initCapacity ->
				new InverseTestHeapMinMaxPriorityQueue<>(createQueueSet(initCapacity)),
				((Comparator<Long>) Long::compareTo).reversed(),
				true}
		});
	}

	private static HeapMinMaxPriorityQueue<TestElement> createQueue(int initCapacity) {
		return new HeapMinMaxPriorityQueue<>(TEST_ELEMENT_PRIORITY_COMPARATOR, initCapacity);
	}

	private static HeapMinMaxPriorityQueueSet<TestElement> createQueueSet(int initCapacity) {
		return new HeapMinMaxPriorityQueueSet<>(TEST_ELEMENT_PRIORITY_COMPARATOR, initCapacity);
	}

	@Parameterized.Parameter(0)
	public Function<Integer, InternalPriorityQueue<TestElement>> queueFactory;

	@Parameterized.Parameter(1)
	public Comparator<Long> comparator;

	@Parameterized.Parameter(2)
	public boolean setSemantics;

	@Override
	protected Comparator<Long> getTestElementPriorityComparator() {
		return comparator;
	}

	@Override
	protected InternalPriorityQueue<TestElement> newPriorityQueue(int initialCapacity) {
		return queueFactory.apply(initialCapacity);
	}

	@Override
	protected boolean testSetSemanticsAgainstDuplicateElements() {
		return setSemantics;
	}

	/**
	 * Reversing head/tail of the queue so that we can reuse existing tests.
	 */
	private static class InverseTestHeapMinMaxPriorityQueue<T extends HeapPriorityQueueElement>
		implements InternalPriorityQueue<T> {

		private final HeapMinMaxPriorityQueue<T> delegate;

		public InverseTestHeapMinMaxPriorityQueue(@Nonnull HeapMinMaxPriorityQueue<T> delegate) {
			this.delegate = delegate;
		}

		@Nonnull
		public <O> O[] toArray(O[] out) {
			return delegate.toArray(out);
		}

		public void clear() {
			delegate.clear();
		}

		@Override
		public void bulkPoll(@Nonnull Predicate<T> canConsume, @Nonnull Consumer<T> consumer) {
			// There is no real #bulkPollLast (for now), so we emulate it to pass the test.
			T element;
			while ((element = delegate.peekLast()) != null && canConsume.test(element)) {
				delegate.pollLast();
				consumer.accept(element);
			}
		}

		@Nullable
		@Override
		public T poll() {
			return delegate.pollLast();
		}

		@Nullable
		@Override
		public T peek() {
			return delegate.peekLast();
		}

		@Override
		public boolean add(@Nonnull T toAdd) {
			delegate.add(toAdd);
			return toAdd.getInternalIndex() == delegate.getMaxElementIndex();
		}

		@Override
		public boolean remove(@Nonnull T toRemove) {
			if (delegate.isEmpty()) {
				// workaround because #getMaxElementIndex doesn't work for empty delegate queue.
				return delegate.remove(toRemove);
			} else {
				boolean result = toRemove.getInternalIndex() == delegate.getMaxElementIndex();
				delegate.remove(toRemove);
				return result;
			}
		}

		@Override
		public boolean isEmpty() {
			return delegate.isEmpty();
		}

		@Override
		public int size() {
			return delegate.size();
		}

		@Override
		public void addAll(@Nullable Collection<? extends T> toAdd) {
			delegate.addAll(toAdd);
		}

		@Nonnull
		@Override
		public CloseableIterator<T> iterator() {
			return delegate.iterator();
		}
	}
}
