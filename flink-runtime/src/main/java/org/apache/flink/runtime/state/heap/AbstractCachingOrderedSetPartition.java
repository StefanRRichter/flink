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

import java.util.Comparator;

public abstract class AbstractCachingOrderedSetPartition<E> implements HeapOrderedSetElement {

	protected final Comparator<E> elementComparator;
	private int pqManagedIndex;

	@SuppressWarnings("unchecked")
	public AbstractCachingOrderedSetPartition(Comparator<E> elementComparator) {
		this.elementComparator = elementComparator;
		this.pqManagedIndex = HeapOrderedSetElement.NOT_CONTAINED;
	}

	public E getFirst() {
		return peekFirstFromCache();
	}

	public E removeFirst() {

		final E first = removeFirstFromCache();

		if (first != null) {

			removeFromBackend(first);

			if (isCacheEmpty()) {
				refillCacheFromBackend();
			}
		}

		checkConsistency();
		return first;
	}

	public boolean add(E toAdd) {
		if (isCacheEmpty()) {
			addToCache(toAdd);
		} else if (elementComparator.compare(toAdd, peekLastFromCache()) < 0) {
			if (isCacheFull()) {
				removeLastFromCache();
			}
			addToCache(toAdd);
		}
		addToBackend(toAdd);
		checkConsistency();
		return peekFirstFromCache() == toAdd; //TODO move into if
	}

	public boolean remove(E toRemove) {

		boolean result = toRemove.equals(peekFirstFromCache());

		removeFromCache(toRemove);
		removeFromBackend(toRemove);
		if (isCacheEmpty()) {
			refillCacheFromBackend();
		}
		checkConsistency();
		return result;
	}

	protected void checkConsistency() {

	}

	@Override
	public int getManagedIndex() {
		return pqManagedIndex;
	}

	@Override
	public void setManagedIndex(int updateIndex) {
		this.pqManagedIndex = updateIndex;
	}

	protected abstract void addToCache(E element);

	protected abstract void removeFromCache(E element);

	protected abstract boolean isCacheFull();

	protected abstract boolean isCacheEmpty();

	protected abstract E removeFirstFromCache();

	protected abstract E removeLastFromCache();

	protected abstract E peekFirstFromCache();

	protected abstract E peekLastFromCache();

	protected abstract void addToBackend(E element);

	protected abstract void removeFromBackend(E element);

	protected abstract void refillCacheFromBackend();

	protected abstract int size();
}

