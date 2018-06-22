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

public abstract class AbstractCachingOrderedSetPartition<E> implements HeapPriorityQueueElement {

	protected final Comparator<E> elementComparator;
	protected final int keyGroupId;
	private boolean backendOnlyElements;
	private int pqManagedIndex;

	@SuppressWarnings("unchecked")
	public AbstractCachingOrderedSetPartition(int keyGroupId, Comparator<E> elementComparator) {
		this.elementComparator = elementComparator;
		this.keyGroupId = keyGroupId;
		this.pqManagedIndex = HeapPriorityQueueElement.NOT_CONTAINED;
		this.backendOnlyElements = true;
	}

	public E getFirst() {

		pull();

		return peekFirstFromCache();
	}

	public E removeFirst() {

		pull();

		final E first = removeFirstFromCache();

		if (first != null) {
			removeFromBackend(first);
		}

		return first;
	}

	public boolean add(E toAdd) {

		pull();

		final E cacheTail = peekLastFromCache();

		boolean newHead;

		if (!backendOnlyElements || cacheTail != null && elementComparator.compare(toAdd, cacheTail) < 0) {
			if (isCacheFull()) {
				removeLastFromCache();
				backendOnlyElements = true;
			}
			addToCache(toAdd);
			newHead = toAdd.equals(peekFirstFromCache());
		} else {
			backendOnlyElements = true;
			newHead = false;
		}

		addToBackend(toAdd);
		return newHead;
	}

	public boolean remove(E toRemove) {

		pull();

		boolean newHead = toRemove.equals(peekFirstFromCache());
		removeFromCache(toRemove);
		removeFromBackend(toRemove);
		return newHead;
	}

	private void pull() {
		if (backendOnlyElements && isCacheEmpty()) {
			backendOnlyElements = refillCacheFromBackend();
		}
	}

	@Override
	public int getInternalIndex() {
		return pqManagedIndex;
	}

	@Override
	public void setInternalIndex(int updateIndex) {
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

	protected abstract boolean refillCacheFromBackend();

	protected abstract int size();
}
