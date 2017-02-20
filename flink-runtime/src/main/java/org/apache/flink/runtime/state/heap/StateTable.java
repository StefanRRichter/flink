/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class StateTable<K, N, ST> extends AbstractStateTable<K, N, ST> {

	/** Map for holding the actual state objects. */
	private final Map<N, Map<K, ST>>[] state;

	/** The offset to the contiguous key groups */
	private final int keyGroupOffset;

	// ------------------------------------------------------------------------
	public StateTable(RegisteredBackendStateMetaInfo<N, ST> metaInfo, KeyGroupRange keyGroupRange) {
		super(metaInfo);
		this.keyGroupOffset = keyGroupRange.getStartKeyGroup();

		@SuppressWarnings("unchecked")
		Map<N, Map<K, ST>>[] state = (Map<N, Map<K, ST>>[]) new Map[keyGroupRange.getNumberOfKeyGroups()];
		this.state = state;
	}

	// ------------------------------------------------------------------------
	//  access to maps
	// ------------------------------------------------------------------------

	public Map<N, Map<K, ST>>[] getState() {
		return state;
	}

	public Map<N, Map<K, ST>> get(int index) {
		final int pos = indexToOffset(index);
		if (pos >= 0 && pos < state.length) {
			return state[pos];
		} else {
			return null;
		}
	}

	public void set(int index, Map<N, Map<K, ST>> map) {
		try {
			state[indexToOffset(index)] = map;
		}
		catch (ArrayIndexOutOfBoundsException e) {
			throw new IllegalArgumentException("Key group index out of range of key group range [" +
					keyGroupOffset + ", " + (keyGroupOffset + state.length) + ").");
		}
	}

	private int indexToOffset(int index) {
		return index - keyGroupOffset;
	}

	// ------------------------------------------------------------------------

	@Override
	public int size() {
		int count = 0;
		for (Map<N, Map<K, ST>> namespaceMap : state) {
			if (null != namespaceMap) {
				for (Map<K, ST> keyMap : namespaceMap.values()) {
					if (null != keyMap) {
						count += keyMap.size();
					}
				}
			}
		}
		return count;
	}

	@Override
	public boolean containsKey(Object key, Object namespace) {
		final int keyGroupIndex = backend.getCurrentKeyGroupIndex();

		Map<N, Map<K, ST>> namespaceMap = get(keyGroupIndex);

		if (namespaceMap == null) {
			return false;
		}

		Map<K, ST> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			return false;
		}

		return keyedMap.containsKey(key);
	}

	@Override
	public ST get(Object key, Object namespace) {

		final int keyGroupIndex = backend.getCurrentKeyGroupIndex();

		Map<N, Map<K, ST>> namespaceMap = get(keyGroupIndex);

		if (namespaceMap == null) {
			return null;
		}

		Map<K, ST> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			return null;
		}

		return keyedMap.get(key);
	}

	@Override
	public void put(K key, N namespace, ST value) {
		putAndGetOld(key, namespace, value);
	}

	@Override
	public ST putAndGetOld(K key, N namespace, ST value) {
		final int keyGroupIndex = backend.getCurrentKeyGroupIndex();

		Map<N, Map<K, ST>> namespaceMap = get(keyGroupIndex);

		if (namespaceMap == null) {
			namespaceMap = new HashMap<>();
			set(keyGroupIndex, namespaceMap);
		}

		Map<K, ST> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			keyedMap = new HashMap<>();
			namespaceMap.put(namespace, keyedMap);
		}

		return keyedMap.put(key, value);
	}

	@Override
	public void remove(Object key, Object namespace) {
		removeAndGetOld(key, namespace);
	}

	@Override
	public ST removeAndGetOld(Object key, Object namespace) {
		final int keyGroupIndex = backend.getCurrentKeyGroupIndex();

		Map<N, Map<K, ST>> namespaceMap = get(keyGroupIndex);

		if (namespaceMap == null) {
			return null;
		}

		Map<K, ST> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			return null;
		}

		ST removed = keyedMap.remove(key);

		if (keyedMap.isEmpty()) {
			namespaceMap.remove(namespace);
		}

		return removed;
	}

	@Override
	public Iterator<StateEntry<K, N, ST>> iterator() {
		throw new UnsupportedOperationException("TODO!");
	}
}
