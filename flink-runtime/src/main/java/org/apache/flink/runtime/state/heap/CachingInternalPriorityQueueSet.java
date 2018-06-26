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
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;

/**
 * @param <E>
 */
public class CachingInternalPriorityQueueSet<E> implements InternalPriorityQueue<E>, HeapPriorityQueueElement {

	private final OrderedSetCache<E> orderedCache;
	private final OrderedSetStore<E> orderedStore;

	private boolean backendOnlyElements;

	private int pqManagedIndex;

	@SuppressWarnings("unchecked")
	public CachingInternalPriorityQueueSet(
		OrderedSetCache<E> orderedCache,
		OrderedSetStore<E> orderedStore) {

		this.pqManagedIndex = HeapPriorityQueueElement.NOT_CONTAINED;
		this.orderedCache = orderedCache;
		this.orderedStore = orderedStore;
		//we are careful and set this to true. could be set to false if we know the backend is empty.
		this.backendOnlyElements = true;
	}

	@Nullable
	@Override
	public E peek() {

		checkRefillCacheFromBackend();

		return orderedCache.peekFirst();
	}

	@Nullable
	@Override
	public E poll() {

		checkRefillCacheFromBackend();

		final E first = orderedCache.removeFirst();

		if (first != null) {
			orderedStore.remove(first);
		}

		validate();
		return first;
	}

	private void validate() {
		if(!orderedCache.isEmpty()) {
			try(final CloseableIterator<E> iterator = orderedStore.orderedIterator()) {
				Preconditions.checkState(orderedCache.peekFirst().equals(iterator.next()));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}


	@Override
	public boolean add(@Nonnull E toAdd) {

		checkRefillCacheFromBackend();

		orderedStore.add(toAdd);

		if (backendOnlyElements) {
			if (orderedCache.isInLowerBound(toAdd)) {
				if (orderedCache.isFull()) {
					orderedCache.removeLast();
				}
				orderedCache.add(toAdd);
				return toAdd.equals(orderedCache.peekFirst());
			} else {
				return false;
			}
		} else {
			if (orderedCache.isFull()) {
				if (orderedCache.isInLowerBound(toAdd)) {
					orderedCache.removeLast();
					orderedCache.add(toAdd);
					backendOnlyElements = true;
				} else {
					return false;
				}
			} else {
				orderedCache.add(toAdd);
			}
			return toAdd.equals(orderedCache.peekFirst());
		}
//		if (!backendOnlyElements || orderedCache.isInLowerBound(toAdd)) {
//			if (orderedCache.isFull()) {
//				orderedCache.removeLast();
//				backendOnlyElements = true;
//			}
//			orderedCache.add(toAdd);
//			validate();
//			return toAdd.equals(orderedCache.peekFirst()) ;
//		} else {
//			backendOnlyElements = true;
//			validate();
//			return false;
//		}
	}

	@Override
	public boolean remove(@Nonnull E toRemove) {

		checkRefillCacheFromBackend();

		boolean newHead = toRemove.equals(orderedCache.peekFirst());
		orderedStore.remove(toRemove);
		orderedCache.remove(toRemove);
		validate();
		return newHead;
	}

	@Override
	public void addAll(@Nullable Collection<? extends E> toAdd) {

		if (toAdd == null) {
			return;
		}

		for (E element : toAdd) {
			add(element);
		}
	}

	public void clear() {
		while (poll() != null) ;
	}

	@Override
	public int size() {
		return orderedStore.size();
	}

	@Override
	public boolean isEmpty() {
		checkRefillCacheFromBackend();
		return orderedCache.isEmpty();
	}

	@Nonnull
	@Override
	public CloseableIterator<E> iterator() {
		return orderedStore.orderedIterator();
	}

	@Override
	public int getInternalIndex() {
		return pqManagedIndex;
	}

	@Override
	public void setInternalIndex(int updateIndex) {
		this.pqManagedIndex = updateIndex;
	}

	private void checkRefillCacheFromBackend() {
		if (backendOnlyElements && orderedCache.isEmpty()) {
			try (final CloseableIterator<E> iterator = orderedStore.orderedIterator()) {
				while (iterator.hasNext() && !orderedCache.isFull()) {
					orderedCache.add(iterator.next());
				}
				backendOnlyElements = iterator.hasNext();
			} catch (Exception e) {
				throw new FlinkRuntimeException("Exception while closing RocksDB iterator.", e);
			}
		}
	}

	public interface OrderedSetCache<E> {

		void add(E element);

		void remove(E element);

		boolean isFull();

		boolean isEmpty();

		boolean isInLowerBound(E toCheck);

		E removeFirst();

		E removeLast();

		E peekFirst();

		E peekLast();
	}

	public interface OrderedSetStore<E> {

		/**
		 *
		 * @param element
		 * @return true if head changed or unknown. false iff not changed.
		 */
		void add(E element);

		/**
		 *
		 * @param element
		 * @return true if head changed or unknown. false iff not changed.
		 */
		void remove(E element);

		int size();

		CloseableIterator<E> orderedIterator();
	}
}
