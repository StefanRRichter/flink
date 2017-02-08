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
import org.apache.flink.api.java.tuple.Tuple3;
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
 *
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
	HashMapEntry<K, N, S>[] table;

	/**
	 * The number of mappings in this hash map.
	 */
	int size;

	/**
	 * Incremented by "structural modifications" to allow (best effort)
	 * detection of concurrent modification.
	 */
	int modCount;

	/**
	 * The current version of this map. Used for copy-on-write mechanics.
	 */
	int mapVersion;

//	/**
//	 * This type serializer is used for copy-on-write of the mutable state objects.
//	 */
//	final TypeSerializer<S> stateTypeSerializer;

	/**
	 * The table is rehashed when its size exceeds this threshold.
	 * The value of this field is generally .75 * capacity, except when
	 * the capacity is zero, as described in the EMPTY_TABLE declaration
	 * above.
	 */
	private int threshold;

	/**
	 * Key group range which goes to this backend
	 */
	final KeyGroupRange keyGroupRange;

	/**
	 * Combined meta information such as name and serializers for this state
	 */
	RegisteredBackendStateMetaInfo<N, S> metaInfo;

	final int totalNumberOfKeyGroups;

	public StateTable(
			RegisteredBackendStateMetaInfo<N, S> metaInfo,
			KeyGroupRange keyGroupRange,
			int totalNumberOfKeyGroups) {
		this(128, metaInfo, keyGroupRange, totalNumberOfKeyGroups);
	}

	/**
	 * Constructs a new {@code HashMap} instance with the specified capacity.
	 *
	 * @param capacity the initial capacity of this hash map.
	 * @param stateTypeSerializer the type serializer for state copy-on-write.
	 * @throws IllegalArgumentException when the capacity is less than zero.
	 */
	public StateTable(
			int capacity,
			RegisteredBackendStateMetaInfo<N, S> metaInfo,
			KeyGroupRange keyGroupRange,
			int totalNumberOfKeyGroups) {

		this.metaInfo = Preconditions.checkNotNull(metaInfo);
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
		this.totalNumberOfKeyGroups = totalNumberOfKeyGroups;
		this.mapVersion = 0;

		if (capacity < 0) {
			throw new IllegalArgumentException("Capacity: " + capacity);
		}
		if (capacity == 0) {
			@SuppressWarnings("unchecked")
			HashMapEntry<K, N, S>[] tab = (HashMapEntry<K, N, S>[]) EMPTY_TABLE;
			table = tab;
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
		makeTable(capacity);
	}

	/**
	 * Constructs a new {@code HashMap} instance with the specified capacity and
	 * load factor.
	 *
	 * @param capacity   the initial capacity of this hash map.
	 * @param loadFactor the initial load factor.
	 * @param stateTypeSerializer the type serializer for state copy-on-write.
	 * @throws IllegalArgumentException when the capacity is less than zero or the load factor is
	 *                                  less or equal to zero or NaN.
	 */
	public StateTable(
			int capacity,
			float loadFactor,
			RegisteredBackendStateMetaInfo<N, S> metaInfo,
			KeyGroupRange keyGroupRange,
			int totalNumberOfKeyGroups) {
		this(capacity, metaInfo, keyGroupRange, totalNumberOfKeyGroups);
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
		return size == 0;
	}

	/**
	 * Returns the number of elements in this map.
	 *
	 * @return the number of elements in this map.
	 */
	public int size() {
		return size;
	}

	/**
	 * Returns the value of the mapping with the specified key/namespace composite key.
	 *
	 * @param key the key.
	 * @param namespace the namespace.
	 * @return the value of the mapping with the specified key/namespace composite key, or {@code null}
	 * if no mapping for the specified key is found.
	 */
	public S get(Object key, Object namespace) {

		assert (null != key && null != namespace);

		int hash = secondaryHash(key, namespace);
		HashMapEntry<K, N, S>[] tab = table;
		for (HashMapEntry<K, N, S> e = tab[hash & (tab.length - 1)]; e != null; e = e.next) {
			K eKey = e.key;
			N eNamespace = e.namespace;
			if ((e.hash == hash && key.equals(eKey) && namespace.equals(eNamespace))) {
				// copy-on-access if we are on an old version
				return e.getStateCopyOnAccess(mapVersion, getStateSerializer());
			}
		}
		return null;
	}

	/**
	 * Returns whether this map contains the specified key/namespace composite key.
	 *
	 * @param key the key in the composite key to search for.
	 * @param namespace the namespace in the composite key to search for.
	 * @return {@code true} if this map contains the specified key/namespace composite key,
	 * {@code false} otherwise.
	 */
	public boolean containsKey(Object key, Object namespace) {

		assert (null != key && null != namespace);

		int hash = secondaryHash(key, namespace);
		HashMapEntry<K, N, S>[] tab = table;
		for (HashMapEntry<K, N, S> e = tab[hash & (tab.length - 1)]; e != null; e = e.next) {
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
	 * @param key the key.
	 * @param namespace the namespace.
	 * @param value the value.
	 * @return the value of any previous mapping with the specified key or
	 * {@code null} if there was no such mapping.
	 */
	public void put(K key, N namespace, S value) {

		assert (null != key && null != namespace);

		int hash = secondaryHash(key, namespace);
		HashMapEntry<K, N, S>[] tab = table;
		int index = hash & (tab.length - 1);
		for (HashMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {
				e.setState(value, mapVersion);
				return;
			}
		}
		modCount++;
		if (size++ > threshold) {
			tab = doubleCapacity();
			index = hash & (tab.length - 1);
		}
		addNewEntry(key, namespace, value, hash, index);
	}

	/**
	 * Maps the specified key/namespace composite key to the specified value.
	 *
	 * @param key the key.
	 * @param namespace the namespace.
	 * @param value the value.
	 * @return the value of any previous mapping with the specified key or
	 * {@code null} if there was no such mapping.
	 */
	public S putAndGetOld(K key, N namespace, S value) {

		assert (null != key && null != namespace);

		int hash = secondaryHash(key, namespace);
		HashMapEntry<K, N, S>[] tab = table;
		int index = hash & (tab.length - 1);
		for (HashMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {
				S oldState = e.getStateCopyOnAccess(mapVersion, getStateSerializer());
				e.state = value;
				return oldState;
			}
		}
		modCount++;
		if (size++ > threshold) {
			tab = doubleCapacity();
			index = hash & (tab.length - 1);
		}
		addNewEntry(key, namespace, value, hash, index);
		return null;
	}

	void addNewEntry(K key, N namespace, S value, int hash, int index) {
		table[index] = new HashMapEntry<>(key, namespace, value, hash, table[index], mapVersion);
	}

	/**
	 * Allocate a table of the given capacity and set the threshold accordingly.
	 *
	 * @param newCapacity must be a power of two
	 */
	private HashMapEntry<K, N, S>[] makeTable(int newCapacity) {
		@SuppressWarnings("unchecked") HashMapEntry<K, N, S>[] newTable
				= (HashMapEntry<K, N, S>[]) new HashMapEntry[newCapacity];
		table = newTable;
		threshold = (newCapacity >> 1) + (newCapacity >> 2); // 3/4 capacity
		return newTable;
	}

	/**
	 * Doubles the capacity of the hash table. Existing entries are placed in
	 * the correct bucket on the enlarged table. If the current capacity is,
	 * MAXIMUM_CAPACITY, this method is a no-op. Returns the table, which
	 * will be new unless we were already at MAXIMUM_CAPACITY.
	 */
	private HashMapEntry<K, N, S>[] doubleCapacity() {
		HashMapEntry<K, N, S>[] oldTable = table;
		int oldCapacity = oldTable.length;
		if (oldCapacity == MAXIMUM_CAPACITY) {
			return oldTable;
		}
		int newCapacity = oldCapacity * 2;
		HashMapEntry<K, N, S>[] newTable = makeTable(newCapacity);
		if (size == 0) {
			return newTable;
		}
		for (int j = 0; j < oldCapacity; j++) {
			/*
			* Rehash the bucket using the minimum number of field writes.
			* This is the most subtle and delicate code in the class.
			*/
			HashMapEntry<K, N, S> e = oldTable[j];
			if (e == null) {
				continue;
			}
			int highBit = e.hash & oldCapacity;
			HashMapEntry<K, N, S> broken = null;
			newTable[j | highBit] = e;
			for (HashMapEntry<K, N, S> n = e.next; n != null; e = n, n = n.next) {
				int nextHighBit = n.hash & oldCapacity;
				if (nextHighBit != highBit) {
					if (broken == null) {
						newTable[j | nextHighBit] = n;
					} else {
						broken.next = n;
					}
					broken = e;
					highBit = nextHighBit;
				}
			}
			if (broken != null) {
				broken.next = null;
			}
		}
		return newTable;
	}

	/**
	 * Removes the mapping with the specified key/namespace composite key from this map.
	 *
	 * @param key the key of the mapping to remove.
	 * @param namespace the namespace of the mapping to remove.
	 * @return the value of the removed mapping or {@code null} if no mapping
	 * for the specified key was found.
	 */
	public S remove(Object key, Object namespace) {

		assert (null != key && null != namespace);

		int hash = secondaryHash(key, namespace);
		HashMapEntry<K, N, S>[] tab = table;
		int index = hash & (tab.length - 1);
		for (HashMapEntry<K, N, S> e = tab[index], prev = null; e != null; prev = e, e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {
				if (prev == null) {
					tab[index] = e.next;
				} else {
					prev.next = e.next;
				}
				modCount++;
				size--;
				return e.state;
			}
		}
		return null;
	}

	/**
	 * Removes all mappings from this hash map, leaving it empty.
	 *
	 * @see #isEmpty
	 * @see #size
	 */
	public void clear() {
		if (size != 0) {
			Arrays.fill(table, null);
			modCount++;
			size = 0;
		}
	}

	/**
	 * Creates a dense snapshot of the hashmap
	 * @return a dense snapshot of the hashmap
	 */
	public Tuple3<K, N, S>[] snapshotDump() {

		Tuple3<K, N, S>[] dump = new Tuple3[size()];
		int pos = 0;

		for (HashMapEntry<K, N, S> entry : table) {
			while (null != entry) {
				dump[pos++] = new Tuple3<>(entry.getKey(), entry.getNamespace(), entry.getState());
				entry = entry.next;
			}
		}

		// increase the map version for CoW
		++mapVersion;
		return dump;
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

		final int hash;

		int version;

		HashMapEntry<K, N, S> next;

		HashMapEntry() {
			this(null, null, null, 0, null, 0);
		}

		HashMapEntry(K key, N namespace, S state, int hash, HashMapEntry<K, N, S> next, int version) {
			this.key = key;
			this.namespace = namespace;
			this.state = state;
			this.hash = hash;
			this.next = next;
			this.version = version;
		}

		@Override
		public final K getKey() {
			return key;
		}

		@Override
		public N getNamespace() {
			return namespace;
		}

		public final S getStateCopyOnAccess(int mapVersion, TypeSerializer<S> copySerializer) {
			if (version != mapVersion) {
				version = mapVersion;
				state = copySerializer.copy(state);
			}
			return state;
		}

		@Override
		public final S getState() {
			return state;
		}

		public final void setState(S value, int mapVersion) {
			// we can update the version every time we replace the state with a new object anyways
			if (value != state) {
				this.state = value;
				this.version = mapVersion;
			}
		}

		public int getVersion() {
			return version;
		}

		public void setVersion(int version) {
			this.version = version;
		}

		public void incrementVersion() {
			++this.version;
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

		int nextIndex;
		HashMapEntry<K, N, S> nextEntry;
		int expectedModCount = modCount;

		HashIterator() {
			this.expectedModCount = modCount;
			this.nextIndex = 0;

			HashMapEntry<K, N, S>[] tab = table;
			HashMapEntry<K, N, S> next = null;
			while (next == null && nextIndex < tab.length) {
				next = tab[nextIndex++];
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
			HashMapEntry<K, N, S>[] tab = table;
			HashMapEntry<K, N, S> next = entryToReturn.next;
			while (next == null && nextIndex < tab.length) {
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
		int h = (31 * key.hashCode() + namespace.hashCode());
		return h ^ (h >>> 16);
	}

	@Override
	public String toString() {
		String initial = "VersionedHashMap{";
		StringBuilder sb = new StringBuilder(initial);
		boolean separatorRequired = false;
		for (HashMapEntry<K, N, S> entry : table) {
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

	public StateTableSnapshot<K, N, S> createSnapshot(TypeSerializer<K> keySerializer, int totalKeyGroups) {
		return new StateTableSnapshot<>(
				snapshotDump(),
				keySerializer,
				metaInfo.getNamespaceSerializer(),
				metaInfo.getStateSerializer(),
				keyGroupRange,
				totalKeyGroups);
	}
}