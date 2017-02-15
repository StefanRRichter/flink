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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Basis for Flink's in-memory state tables with copy-on-write support. This map does not support null values for
 * key or namespace.
 * <p>
 * This class was originally based on the {@link java.util.HashMap} implementation of the Android JDK.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of value
 */
public class StateTable<K, N, S> implements Iterable<StateTableEntry<K, N, S>> {

	/**
	 * Min capacity (other than zero) for a HashMap. Must be a power of two
	 * greater than 1 (and less than 1 << 30).
	 */
	private static final int MINIMUM_CAPACITY = 4;
	/**
	 * Max capacity for a HashMap. Must be a power of two >= MINIMUM_CAPACITY.
	 */
	private static final int MAXIMUM_CAPACITY = 1 << 30;
	/**
	 * An empty table shared by all zero-capacity maps (typically from default
	 * constructor). It is never written to, and replaced on first put. Its size
	 * is set to half the minimum, so that the first resize will create a
	 * minimum-sized table.
	 */
	private static final HashMapEntry[] EMPTY_TABLE = new HashMapEntry[MINIMUM_CAPACITY >>> 1];
	/**
	 * The default load factor. Note that this implementation ignores the
	 * load factor, but cannot do away with it entirely because it's
	 * mentioned in the API.
	 * <p>
	 * <p>Note that this constant has no impact on the behavior of the program,
	 * but it is emitted as part of the serialized form. The load factor of
	 * .75 is hardwired into the program, which uses cheap shifts in place of
	 * expensive division.
	 */
	static final float DEFAULT_LOAD_FACTOR = .75F;
	/**
	 * The hash table.
	 */
	final HashMapEntry<K, N, S>[][] tables;

	/**
	 * The number of mappings in this hash map.
	 */
	final int[] size;

	/**
	 * Incremented by "structural modifications" to allow (best effort)
	 * detection of concurrent modification.
	 */
	int modCount;

	/**
	 * The current version of this map. Used for copy-on-write mechanics.
	 */
	int mapVersion;

	/**
	 * The highest version of this map that is still required by a snapshot.
	 */
	int highestRequiredSnapshotVersion;

	/**
	 *
	 */
	int rehashPos;

	/**
	 * The table is rehashed when its size exceeds this threshold.
	 * The value of this field is generally .75 * capacity, except when
	 * the capacity is zero, as described in the EMPTY_TABLE declaration
	 * above.
	 */
	private int threshold;

	/**
	 * Combined meta information such as name and serializers for this state
	 */
	RegisteredBackendStateMetaInfo<N, S> metaInfo;

	/**
	 * Last namespace that was actually inserted. this is a small optimization to decrease duplicate namespace objects.
	 */
	N lastNamespace;

	/**
	 * Constructs a new {@code StateTable} with default capacity of 128.
	 *
	 * @param metaInfo the meta information, including the type serializer for state copy-on-write.
	 */
	public StateTable(
			RegisteredBackendStateMetaInfo<N, S> metaInfo) {
		this(128, metaInfo);
	}

	/**
	 * Constructs a new {@code StateTable} instance with the specified capacity.
	 *
	 * @param capacity the initial capacity of this hash map.
	 * @param metaInfo the meta information, including the type serializer for state copy-on-write.
	 * @throws IllegalArgumentException when the capacity is less than zero.
	 */
	public StateTable(
			int capacity,
			RegisteredBackendStateMetaInfo<N, S> metaInfo) {

		this.metaInfo = Preconditions.checkNotNull(metaInfo);
		this.mapVersion = 0;
		this.highestRequiredSnapshotVersion = mapVersion;
		this.tables = new HashMapEntry[][]{EMPTY_TABLE, EMPTY_TABLE};
		this.size = new int[2];
		this.rehashPos = 0;

		if (capacity < 0) {
			throw new IllegalArgumentException("Capacity: " + capacity);
		}
		if (capacity == 0) {
			threshold = -1; // Forces first put() to replace EMPTY_TABLE
			return;
		}
		if (capacity < MINIMUM_CAPACITY) {
			capacity = MINIMUM_CAPACITY;
		} else if (capacity > MAXIMUM_CAPACITY) {
			capacity = MAXIMUM_CAPACITY;
		} else {
			capacity = MathUtils.roundUpToPowerOfTwo(capacity);
		}
		makeTable(capacity, 0);
	}

	/**
	 * Constructs a new {@code StateTable}} instance with the specified capacity and
	 * load factor.
	 *
	 * @param capacity   the initial capacity of this hash map.
	 * @param loadFactor the initial load factor.
	 * @param metaInfo   the meta information, including the type serializer for state copy-on-write.
	 * @throws IllegalArgumentException when the capacity is less than zero or the load factor is
	 *                                  less or equal to zero or NaN.
	 */
	public StateTable(
			int capacity,
			float loadFactor,
			RegisteredBackendStateMetaInfo<N, S> metaInfo) {

		this(capacity, metaInfo);
		if (loadFactor <= 0 || Float.isNaN(loadFactor)) {
			throw new IllegalArgumentException("Load factor: " + loadFactor);
		}
		/*
		 * Note that this implementation ignores loadFactor; it always uses
		* a load factor of 3/4. This simplifies the code and generally
		 * improves performance.
		*/
	}

	/**
	 * Returns whether this map is empty.
	 *
	 * @return {@code true} if this map has no elements, {@code false}
	 * otherwise.
	 * @see #size()
	 */
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Returns the number of elements in this map.
	 *
	 * @return the number of elements in this map.
	 */
	public int size() {
		return size[0] + size[1];
	}

	/**
	 * Returns the value of the mapping with the specified key/namespace composite key.
	 *
	 * @param key       the key.
	 * @param namespace the namespace.
	 * @return the value of the mapping with the specified key/namespace composite key, or {@code null}
	 * if no mapping for the specified key is found.
	 */
	public S get(Object key, Object namespace) {

		assert (null != key && null != namespace);

		if (isRehashing()) {
			incrementalRehash();
		}

		final int globalVersion = highestRequiredSnapshotVersion;
		HashMapEntry<K, N, S>[] tab = tables[0];
		int hash = secondaryHash(key, namespace);
		int index = hash & (tab.length - 1);

		if (index < rehashPos) {
			tab = tables[1];
			index = hash & (tab.length - 1);
		}

		for (HashMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			K eKey = e.key;
			N eNamespace = e.namespace;
			if ((e.hash == hash && key.equals(eKey) && namespace.equals(eNamespace))) {

				// copy-on-access if we are on an old version
				if (e.stateVersion < globalVersion) {
					if (e.entryVersion < globalVersion) {
						e = handleEntryChainMultiVersioning(tab, hash & (tab.length - 1), e);
					}
					e.stateVersion = mapVersion;
					e.state = getStateSerializer().copy(e.state);
				}

				return e.state;
			}
		}

		return null;
	}

	private HashMapEntry<K, N, S> handleEntryChainMultiVersioning(HashMapEntry<K, N, S>[] tab, int tableIdx, HashMapEntry<K, N, S> untilEntry) {

		final int required = highestRequiredSnapshotVersion;

		HashMapEntry<K, N, S> current = tab[tableIdx];
		HashMapEntry<K, N, S> copy;

		if (current.entryVersion < required) {
			copy = new HashMapEntry<>(current, mapVersion);
			tab[tableIdx] = copy;
		} else {
			// nothing to do, just advance copy to current
			copy = current;
		}

		while (current != untilEntry) {

			//advance current
			current = current.next;

			if (current.entryVersion < required) {
				// copy and advance the current's copy
				copy.next = new HashMapEntry<>(current, mapVersion);
				copy = copy.next;
			} else {
				// nothing to do, just advance copy to current
				copy = current;
			}
		}
		return copy;
	}

	/**
	 * Returns whether this map contains the specified key/namespace composite key.
	 *
	 * @param key       the key in the composite key to search for.
	 * @param namespace the namespace in the composite key to search for.
	 * @return {@code true} if this map contains the specified key/namespace composite key,
	 * {@code false} otherwise.
	 */
	public boolean containsKey(Object key, Object namespace) {

		assert (null != key && null != namespace);

		if (isRehashing()) {
			incrementalRehash();
		}

		HashMapEntry<K, N, S>[] tab = tables[0];
		int hash = secondaryHash(key, namespace);
		int index = hash & (tab.length - 1);

		if (index < rehashPos) {
			tab = tables[1];
			index = hash & (tab.length - 1);
		}

		for (HashMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			K eKey = e.key;
			N eNamespace = e.namespace;
			if ((e.hash == hash && key.equals(eKey) && namespace.equals(eNamespace))) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Maps the specified key/namespace composite key to the specified value.
	 *
	 * @param key       the key.
	 * @param namespace the namespace.
	 * @param value     the value.
	 * @return the value of any previous mapping with the specified key or
	 * {@code null} if there was no such mapping.
	 */
	public void put(K key, N namespace, S value) {

		assert (null != key && null != namespace);

		if (isRehashing()) {
			incrementalRehash();
		}

		int insertTableIdx = 0;
		HashMapEntry<K, N, S>[] tab = tables[0];
		int hash = secondaryHash(key, namespace);
		int index = hash & (tab.length - 1);

		if (index < rehashPos) {
			insertTableIdx = 1;
			tab = tables[1];
			index = hash & (tab.length - 1);
		}

		int globalVersion = highestRequiredSnapshotVersion;

		for (HashMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {

				if (e.entryVersion < globalVersion) {
					e = handleEntryChainMultiVersioning(tab, index, e);
				}

				e.state = value;
				e.stateVersion = mapVersion;
				return;
			}
		}

		++modCount;
		if (size() > threshold) { //TODO size of master table is actually enough if we ensure no rehash during a rehash
			doubleCapacity();
		}
		addNewEntry(insertTableIdx, key, namespace, value, hash);
	}

	/**
	 * Maps the specified key/namespace composite key to the specified value.
	 *
	 * @param key       the key.
	 * @param namespace the namespace.
	 * @param value     the value.
	 * @return the value of any previous mapping with the specified key or
	 * {@code null} if there was no such mapping.
	 */
	public S putAndGetOld(K key, N namespace, S value) {

		assert (null != key && null != namespace);

		if (isRehashing()) {
			incrementalRehash();
		}

		int insertTableIdx = 0;
		HashMapEntry<K, N, S>[] tab = tables[0];
		int hash = secondaryHash(key, namespace);
		int index = hash & (tab.length - 1);

		if (index < rehashPos) {
			insertTableIdx = 1;
			tab = tables[1];
			index = hash & (tab.length - 1);
		}

		int globalVersion = highestRequiredSnapshotVersion;

		for (HashMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {

				S oldState;

				if (e.entryVersion < globalVersion) {
					e = handleEntryChainMultiVersioning(tab, index, e);
				}

				if (e.stateVersion < globalVersion) {
					oldState = getStateSerializer().copy(e.state);
					e.stateVersion = mapVersion;
				} else {
					oldState = e.state;
				}


				e.state = value;
				return oldState;
			}
		}

		++modCount;
		if (size() > threshold) { //TODO size of master table is actually enough if we ensure no rehash during a rehash
			doubleCapacity();
		}

		addNewEntry(insertTableIdx, key, namespace, value, hash);
		return null;
	}

	void addNewEntry(int tabIdx, K key, N namespace, S value, int hash) {

		// small optimization that aims to avoid holding references on duplicate namespace objects
		if (namespace.equals(lastNamespace)) {
			namespace = lastNamespace;
		} else {
			lastNamespace = namespace;
		}

		HashMapEntry<K, N, S>[] insertTable = tables[tabIdx];
		int index = hash & (insertTable.length - 1);
		insertTable[index] = new HashMapEntry<>(key, namespace, value, hash, insertTable[index], mapVersion, mapVersion);

		++size[tabIdx];
	}

	/**
	 * Allocate a table of the given capacity and set the threshold accordingly.
	 *
	 * @param newCapacity must be a power of two
	 */
	private void makeTable(int newCapacity, int tableIdx) {
		@SuppressWarnings("unchecked") HashMapEntry<K, N, S>[] newTable
				= (HashMapEntry<K, N, S>[]) new HashMapEntry[newCapacity];
		tables[tableIdx] = newTable;
		threshold = (newCapacity >> 1) + (newCapacity >> 2); // 3/4 capacity
	}

	/**
	 * Doubles the capacity of the hash table. Existing entries are placed in
	 * the correct bucket on the enlarged table. If the current capacity is,
	 * MAXIMUM_CAPACITY, this method is a no-op. Returns the table, which
	 * will be new unless we were already at MAXIMUM_CAPACITY.
	 */
	private void doubleCapacity() {

		Preconditions.checkState(!isRehashing());

		HashMapEntry<K, N, S>[] oldTable = tables[0];

		int oldCapacity = oldTable.length;

		if (oldCapacity == MAXIMUM_CAPACITY) {
			return;
		}

		int newCapacity = oldCapacity * 2;
		makeTable(newCapacity, 1);
	}

	private boolean isRehashing() {
		// if we rehash, the secondary table is not empty
		return tables[1] != EMPTY_TABLE;
	}

	/**
	 * Removes the mapping with the specified key/namespace composite key from this map.
	 *
	 * @param key       the key of the mapping to remove.
	 * @param namespace the namespace of the mapping to remove.
	 * @return the value of the removed mapping or {@code null} if no mapping
	 * for the specified key was found.
	 */
	public void remove(Object key, Object namespace) {

		assert (null != key && null != namespace);

		if (isRehashing()) {
			incrementalRehash();
		}

		int removeTableIdx = 0;
		HashMapEntry<K, N, S>[] tab = tables[0];
		int hash = secondaryHash(key, namespace);
		int index = hash & (tab.length - 1);

		if (index < rehashPos) {
			removeTableIdx = 1;
			tab = tables[1];
			index = hash & (tab.length - 1);
		}

		for (HashMapEntry<K, N, S> e = tab[index], prev = null; e != null; prev = e, e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {
				if (prev == null) {
					tab[index] = e.next;
				} else {
					if (prev.entryVersion != mapVersion) {
						prev = handleEntryChainMultiVersioning(tab, index, prev);
					}
					prev.next = e.next;
				}
				++modCount;
				--size[removeTableIdx];
				return;
			}
		}

	}

	/**
	 * Removes the mapping with the specified key/namespace composite key from this map.
	 *
	 * @param key       the key of the mapping to remove.
	 * @param namespace the namespace of the mapping to remove.
	 * @return the value of the removed mapping or {@code null} if no mapping
	 * for the specified key was found.
	 */
	public S removeAndGetOld(Object key, Object namespace) {

		assert (null != key && null != namespace);

		if (isRehashing()) {
			incrementalRehash();
		}

		int removeTableIdx = 0;
		HashMapEntry<K, N, S>[] tab = tables[0];
		int hash = secondaryHash(key, namespace);
		int index = hash & (tab.length - 1);

		if (index < rehashPos) {
			removeTableIdx = 1;
			tab = tables[1];
			index = hash & (tab.length - 1);
		}

		int globalVersion = mapVersion;
		for (HashMapEntry<K, N, S> e = tab[index], prev = null; e != null; prev = e, e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {
				if (prev == null) {
					tab[index] = e.next;
				} else {
					if (prev.entryVersion != globalVersion) {
						prev = handleEntryChainMultiVersioning(tab, index, prev);
					}
					prev.next = e.next;
				}
				++modCount;
				--size[removeTableIdx];
				return e.stateVersion == globalVersion ? e.state : getStateSerializer().copy(e.state);
			}
		}

		return null;
	}

//	/**
//	 * Removes all mappings from this hash map, leaving it empty.
//	 *
//	 * @see #isEmpty
//	 * @see #size
//	 */
//	public void clear() {
//
//		if (size[0] != 0) {
//			size[0] = 0;
//			Arrays.fill(tables[0], null);
//		}
//
//		tables[1] = EMPTY_TABLE;
//		size[1] = 0;
//
//		++modCount;
//	}

	private void incrementalRehash() {

		HashMapEntry<K, N, S>[] oldTable = tables[0];
		HashMapEntry<K, N, S>[] newTable = tables[1];

		int oldCapacity = oldTable.length;
		int newMask = newTable.length - 1;
		int requiredVersion = highestRequiredSnapshotVersion;
		int j = rehashPos;
		int transferred = 0;
		while (transferred < 8) {

			HashMapEntry<K, N, S> e = oldTable[j];

			while (e != null) {
				if (e.entryVersion < requiredVersion) {
					e = new HashMapEntry<>(e, mapVersion);
				}
				HashMapEntry<K, N, S> n = e.next;
				int pos = e.hash & newMask;
				e.next = newTable[pos];
				newTable[pos] = e;
				e = n;
				++transferred;
			}

			oldTable[j] = null;
			if (++j == oldCapacity) {
				//rehash complete
				tables[0] = newTable;
				tables[1] = EMPTY_TABLE;
				size[0] += size[1];
				size[1] = 0;
				rehashPos = 0;
				return;
			}
		}

		size[0] -= transferred;
		size[1] += transferred;
		rehashPos = j;
	}

	public HashMapEntry<K, N, S>[] snapshotTable() {
		// increase the map version for CoW
		++mapVersion;
		highestRequiredSnapshotVersion = mapVersion;
		HashMapEntry<K, N, S>[] table = tables[0];
		if (isRehashing()) {
			int rhp = rehashPos;
			HashMapEntry<K, N, S>[] copy = new HashMapEntry[rhp + table.length];
			int len = table.length - rhp;
			System.arraycopy(table, rhp, copy, 0, len);
			table = tables[1];
			System.arraycopy(table, 0, copy, len, rhp);
			System.arraycopy(table, table.length >>> 1, copy, len + rhp, rhp);
			return copy;
		} else {
			return Arrays.copyOf(table, table.length);
		}
	}

//	/**
//	 * Creates a dense snapshot of the hashmap
//	 *
//	 * @return a dense snapshot of the hashmap
//	 */
//	public Tuple3<K, N, S>[] snapshotDump() {//TODO return snapshot version
//
//		Tuple3<K, N, S>[] dump = new Tuple3[size()];
//		int pos = 0;
//
//		for (int i = 0; i <= 1; ++i) {
//			HashMapEntry<K, N, S>[] tab = tables[i];
//			for (HashMapEntry<K, N, S> entry : tab) {
//				while (null != entry) {
//					dump[pos++] = new Tuple3<>(entry.getKey(), entry.getNamespace(), entry.getState());
//					entry = entry.next;
//				}
//			}
//		}
//
//		// increase the map version for CoW
//		++mapVersion;
//		highestRequiredSnapshotVersion = mapVersion;
//		return dump;
//	}

	public void releaseSnapshot(int snapshotVersion) {
		if (snapshotVersion == highestRequiredSnapshotVersion) {
			highestRequiredSnapshotVersion = 0;
		}
	}

	@Override
	public Iterator<StateTableEntry<K, N, S>> iterator() {
		return new HashIterator();
	}

	/**
	 * Entry iterator.
	 *
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 * @param <S> type of state.
	 */
	static class HashMapEntry<K, N, S> implements StateTableEntry<K, N, S> {

		final K key;
		final N namespace;
		S state;
		HashMapEntry<K, N, S> next;

		final int hash;

		int entryVersion;
		int stateVersion;


		HashMapEntry() {
			this(null, null, null, 0, null, 0, 0);
		}

		HashMapEntry(HashMapEntry<K, N, S> other, int entryVersion) {
			this(other.key, other.namespace, other.state, other.hash, other.next, entryVersion, other.stateVersion);
		}

		HashMapEntry(K key, N namespace, S state, int hash, HashMapEntry<K, N, S> next, int entryVersion, int stateVersion) {
			this.key = key;
			this.namespace = namespace;
			this.state = state;
			this.hash = hash;
			this.next = next;
			this.entryVersion = entryVersion;
			this.stateVersion = stateVersion;
		}

		@Override
		public final K getKey() {
			return key;
		}

		@Override
		public N getNamespace() {
			return namespace;
		}

//		public final S getStateCopyOnAccess(int expectedVersion, TypeSerializer<S> copySerializer) {
//			if (version < expectedVersion) {
//				// if our version does not meet the required version, we perform a copy and bump the version
//				version = expectedVersion;
//				state = copySerializer.copy(state);
//			}
//			return state;
//		}

		@Override
		public final S getState() {
			return state;
		}

		public final void setState(S value, int mapVersion) {
			// we can update the version every time we replace the state with a new object anyways
			if (value != state) {
				this.state = value;
				this.stateVersion = mapVersion;
			}
		}


		public int getStateVersion() {
			return stateVersion;
		}

		public void setStateVersion(int stateVersion) {
			this.stateVersion = stateVersion;
		}

		public int getEntryVersion() {
			return entryVersion;
		}

		public void setEntryVersion(int entryVersion) {
			this.entryVersion = entryVersion;
		}

		public void incrementVersion() {
			++this.entryVersion;
		}

		@Override
		public final boolean equals(Object o) {
			if (!(o instanceof HashMapEntry)) {
				return false;
			}
			HashMapEntry<?, ?, ?> e = (HashMapEntry<?, ?, ?>) o;
			return e.getKey().equals(key)
					&& e.getNamespace().equals(namespace)
					&& nullAwareEqual(e.getState(), state);
		}

		@Override
		public final int hashCode() {
			return (key.hashCode() ^ namespace.hashCode()) ^ (state == null ? 0 : state.hashCode());
		}

		@Override
		public final String toString() {
			return "(" + key + "|" + namespace + ")=" + state;
		}
	}

	private class HashIterator implements Iterator<StateTableEntry<K, N, S>> {

		int tabIdx;
		int nextIndex;
		HashMapEntry<K, N, S> nextEntry;
		int expectedModCount = modCount;

		HashIterator() {
			this.expectedModCount = modCount;
			this.nextIndex = 0;
			this.tabIdx = 0;

			HashMapEntry<K, N, S>[] tab = tables[0];
			HashMapEntry<K, N, S> next = null;

			while (next == null && nextIndex < tab.length) {
				next = tab[nextIndex++];
			}

			if (next == null) {
				tabIdx = 1;
				tab = tables[1];
				nextIndex = 0;
				while (next == null && nextIndex < tab.length) {
					next = tab[nextIndex++];
				}
			}

			nextEntry = next;
		}

		@Override
		public boolean hasNext() {
			return nextEntry != null;
		}

		@Override
		public HashMapEntry<K, N, S> next() {
			if (modCount != expectedModCount) {
				throw new ConcurrentModificationException();
			}
			if (nextEntry == null) {
				throw new NoSuchElementException();
			}
			HashMapEntry<K, N, S> entryToReturn = nextEntry;
			HashMapEntry<K, N, S>[] tab = tables[tabIdx];
			HashMapEntry<K, N, S> next = entryToReturn.next;
			while (next == null) {
				if (nextIndex < tab.length) {
					if (tabIdx < 1) {
						tabIdx = 1;
						tab = tables[1];
						nextIndex = 0;
					} else {
						break;
					}
				}
				next = tab[nextIndex++];
			}
			nextEntry = next;
			return entryToReturn;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Read-only iterator");
		}
	}

	private static int secondaryHash(Object key, Object namespace) {
		int code = key.hashCode() ^ namespace.hashCode();
		code ^= code >>> 16;
		code *= 0x85ebca6b;
		code ^= code >>> 13;
		code *= 0xc2b2ae35;
		code ^= code >>> 16;
		return code;
	}

	@Override
	public String toString() {
		String initial = "VersionedHashMap{";
		StringBuilder sb = new StringBuilder(initial);
		boolean separatorRequired = false;
		for (int i = 0; i <= 1; ++i) {
			HashMapEntry<K, N, S>[] tab = tables[i];
			for (HashMapEntry<K, N, S> entry : tab) {
				while (null != entry) {

					if (separatorRequired) {
						sb.append(", ");
					} else {
						separatorRequired = true;
					}

					sb.append(entry.toString());
					entry = entry.next;
				}
			}
		}
		sb.append('}');
		return sb.toString();
	}

	private static boolean nullAwareEqual(Object a, Object b) {
		return a == b || (a != null && a.equals(b));
	}


	//------------------------------------------------------------------------------------------------------------------

	public TypeSerializer<S> getStateSerializer() {
		return metaInfo.getStateSerializer();
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return metaInfo.getNamespaceSerializer();
	}

	public RegisteredBackendStateMetaInfo<N, S> getMetaInfo() {
		return metaInfo;
	}

	public void setMetaInfo(RegisteredBackendStateMetaInfo<N, S> metaInfo) {
		this.metaInfo = metaInfo;
	}

	public StateTableSnapshot<K, N, S> createSnapshot(
			TypeSerializer<K> keySerializer,
			KeyGroupRange keyGroupRange,
			int totalKeyGroups) {

		return new StateTableSnapshot<>(
				snapshotTable(),
				size(),
				keySerializer,
				metaInfo.getNamespaceSerializer(),
				metaInfo.getStateSerializer(),
				keyGroupRange,
				totalKeyGroups);
	}
}