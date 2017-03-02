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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @param <K>
 * @param <N>
 * @param <S>
 */
public class NestedMapsStateTable<K, N, S> extends AbstractStateTable<K, N, S> {

	/**
	 * Map for holding the actual state objects.
	 */
	private final Map<N, Map<K, S>>[] state;

	/**
	 * The offset to the contiguous key groups
	 */
	private final int keyGroupOffset;

	// ------------------------------------------------------------------------
	public NestedMapsStateTable(KeyContext<K> keyContext, RegisteredBackendStateMetaInfo<N, S> metaInfo) {
		super(keyContext, metaInfo);
		this.keyGroupOffset = keyContext.getKeyGroupRange().getStartKeyGroup();

		@SuppressWarnings("unchecked")
		Map<N, Map<K, S>>[] state = (Map<N, Map<K, S>>[]) new Map[keyContext.getNumberOfKeyGroups()];
		this.state = state;
	}

	// ------------------------------------------------------------------------
	//  access to maps
	// ------------------------------------------------------------------------

	public Map<N, Map<K, S>>[] getState() {
		return state;
	}

	@VisibleForTesting
	public Map<N, Map<K, S>> getMapForKeyGroup(int keyGroupIndex) {
		final int pos = indexToOffset(keyGroupIndex);
		if (pos >= 0 && pos < state.length) {
			return state[pos];
		} else {
			return null;
		}
	}

	public void setMapForKeyGroup(int index, Map<N, Map<K, S>> map) {
		try {
			state[indexToOffset(index)] = map;
		} catch (ArrayIndexOutOfBoundsException e) {
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
		for (Map<N, Map<K, S>> namespaceMap : state) {
			if (null != namespaceMap) {
				for (Map<K, S> keyMap : namespaceMap.values()) {
					if (null != keyMap) {
						count += keyMap.size();
					}
				}
			}
		}
		return count;
	}

	@Override
	public S get(Object namespace) {
		return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public boolean containsKey(Object namespace) {
		return containsKey(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public void put(N namespace, S state) {
		put(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace, state);
	}

	@Override
	public S putAndGetOld(N namespace, S state) {
		return putAndGetOld(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace, state);
	}

	@Override
	public void remove(Object namespace) {
		remove(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public S removeAndGetOld(Object namespace) {
		return removeAndGetOld(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public S get(Object key, Object namespace) {
		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, keyContext.getNumberOfKeyGroups());
		return get(key, keyGroup, namespace);
	}

	// ------------------------------------------------------------------------

	boolean containsKey(Object key, int keyGroupIndex, Object namespace) {

		Map<N, Map<K, S>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			return false;
		}

		Map<K, S> keyedMap = namespaceMap.get(namespace);

		return keyedMap != null && keyedMap.containsKey(key);
	}

	S get(Object key, int keyGroupIndex, Object namespace) {

		Map<N, Map<K, S>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			return null;
		}

		Map<K, S> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			return null;
		}

		return keyedMap.get(key);
	}

	@Override
	void put(K key, int keyGroupIndex, N namespace, S value) {
		putAndGetOld(key, keyGroupIndex, namespace, value);
	}

	private S putAndGetOld(K key, int keyGroupIndex, N namespace, S value) {

		Map<N, Map<K, S>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			namespaceMap = new HashMap<>();
			setMapForKeyGroup(keyGroupIndex, namespaceMap);
		}

		Map<K, S> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			keyedMap = new HashMap<>();
			namespaceMap.put(namespace, keyedMap);
		}

		return keyedMap.put(key, value);
	}

	private void remove(Object key, int keyGroupIndex, Object namespace) {
		removeAndGetOld(key, keyGroupIndex, namespace);
	}

	private S removeAndGetOld(Object key, int keyGroupIndex, Object namespace) {

		Map<N, Map<K, S>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			return null;
		}

		Map<K, S> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			return null;
		}

		S removed = keyedMap.remove(key);

		if (keyedMap.isEmpty()) {
			namespaceMap.remove(namespace);
		}

		return removed;
	}

	@Override
	public int sizeOfNamespace(Object namespace) {
		int count = 0;
		for (Map<N, Map<K, S>> namespaceMap : state) {
			if (null != namespaceMap) {
				Map<K, S> keyMap = namespaceMap.get(namespace);
				count += keyMap != null ? keyMap.size() : 0;
			}
		}

		return count;
	}

	// snapshots ---------------------------------------------------------------------------------------------------

	private static <K, N, S> int countMappingsInKeyGroup(final Map<N, Map<K, S>> keyGroupMap) {
		int count = 0;
		for (Map<K, S> namespaceMap : keyGroupMap.values()) {
			count += namespaceMap.size();
		}

		return count;
	}

	@Override
	public NestedMapsStateTableSnapshot<K, N, S> createSnapshot() {
		return new NestedMapsStateTableSnapshot<>(this);
	}

	static class NestedMapsStateTableSnapshot<K, N, S>
			extends StateTableSnapshot<K, N, S, NestedMapsStateTable<K, N, S>> {

		NestedMapsStateTableSnapshot(NestedMapsStateTable<K, N, S> owningTable) {
			super(owningTable);
		}

		@Override
		public void writeMappingsInKeyGroup(DataOutputView dov, int keyGroupId) throws IOException {
			final Map<N, Map<K, S>> keyGroupMap = owningStateTable.getMapForKeyGroup(keyGroupId);
			if (null != keyGroupMap) {
				TypeSerializer<K> keySerializer = owningStateTable.keyContext.getKeySerializer();
				TypeSerializer<N> namespaceSerializer = owningStateTable.metaInfo.getNamespaceSerializer();
				TypeSerializer<S> stateSerializer = owningStateTable.metaInfo.getStateSerializer();
				dov.writeInt(countMappingsInKeyGroup(keyGroupMap));
				for (Map.Entry<N, Map<K, S>> namespaceEntry : keyGroupMap.entrySet()) {
					final N namespace = namespaceEntry.getKey();
					final Map<K, S> namespaceMap = namespaceEntry.getValue();

					for (Map.Entry<K, S> keyEntry : namespaceMap.entrySet()) {
						namespaceSerializer.serialize(namespace, dov);
						keySerializer.serialize(keyEntry.getKey(), dov);
						stateSerializer.serialize(keyEntry.getValue(), dov);
					}
				}
			} else {
				dov.writeInt(0);
			}
		}
	}
}
