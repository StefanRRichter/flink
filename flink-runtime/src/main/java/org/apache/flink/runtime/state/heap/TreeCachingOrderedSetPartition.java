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

public abstract class TreeCachingOrderedSetPartition<E> extends AbstractCachingOrderedSetPartition<E> {

	protected final ObjectAVLTreeSet<E> cache;
	protected final int capacity;

	public TreeCachingOrderedSetPartition(int keyGroupId, Comparator<E> elementComparator, int capacity) {
		super(keyGroupId, elementComparator);
		this.cache = new ObjectAVLTreeSet<>(elementComparator);
		this.capacity = capacity;
	}

	@Override
	protected void addToCache(E element) {
		cache.add(element);
	}

	@Override
	protected void removeFromCache(E element) {
		cache.remove(element);
	}

	@Override
	protected boolean isCacheFull() {
		return cache.size() == capacity;
	}

	@Override
	protected boolean isCacheEmpty() {
		return cache.isEmpty();
	}

	@Override
	protected E removeFirstFromCache() {
		if (cache.isEmpty()) {
			return null;
		}
		final E first = cache.first();
		cache.remove(first);
		return first;
	}

	@Override
	protected E removeLastFromCache() {
		if (cache.isEmpty()) {
			return null;
		}
		final E last = cache.last();
		cache.remove(last);
		return last;
	}

	@Override
	protected E peekFirstFromCache() {
		return !cache.isEmpty() ? cache.first() : null;
	}

	@Override
	protected E peekLastFromCache() {
		return !cache.isEmpty() ? cache.last() : null;
	}
}
