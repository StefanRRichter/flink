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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Basis for Flink's in-memory state tables with copy-on-write support. This map does not support null values for
 * key or namespace.
 *
 * <p>
 * StateTable sacrifices some peak performance and memory efficiency for features like incremental rehashing and
 * asynchronous snapshots through copy-on-write. Copy-on-write tries to minimize the amount of copying by maintaining
 * version meta data for both, the map structure and the state objects. However, we must often proactively copy state
 * objects when we hand them to the user.
 *
 * <p>
 * As for any state backend, user should not keep references on state objects that they obtained from state backends
 * outside the scope of the user function calls.
 *
 * <p>
 * Some brief maintenance notes:
 *
 * 1) Flattening the underlying data structure from a nested maps (namespace) -> (key) -> (state) to one flat map
 * (key, namespace) -> (state) brings certain performance trade-offs. In theory, the flat map has one less level of
 * indirection compared to the nested map. However, the nested map naturally de-duplicates namespace objects for which
 * #equals() is true. This leads to potentially a lot of redundant namespace objects for the flattened version. Those,
 * in turn, can again introduce more cache misses because we need to follow the namespace object on all operations to
 * ensure entry identities. Obviously, copy-on-write can also add memory overhead. So does the meta data to track
 * copy-on-write requirement (state and entry versions on {@link StateTableEntry}).
 * <p>
 * 2) A flat map structure is a lot easier when it comes to tracking copy-on-write of the map structure.
 * <p>
 * 3) Nested structure had the (never used) advantage that we can easily drop and iterate whole namespaces. This could
 * give locality advantages for certain access pattern, e.g. iterating a namespace.
 * <p>
 * 4) Serialization format is changed from namespace-prefix compressed (as naturally provided from the old nested
 * structure) to making all entries self contained as (key, namespace, state).
 * <p>
 * 5) We got rid of having multiple nested tables, one for each key-group. Instead, we partition state into key-groups
 * on-the-fly, during the asynchronous part of a snapshot.
 * <p>
 * 6) Currently, a state table can only grow, but never shrinks on low load. We could easily add this if required.
 * <p>
 * 7) Heap based state backends like this can easily cause a lot of GC activity. Besides using G1 as garbage collector,
 * we should provide an additional state backend that operates on off-heap memory. This would sacrifice peak performance
 * (due to de/serialization of objects) for a lower, but more constant throughput and potentially huge simplifications
 * w.r.t. copy-on-write.
 * <p>
 * 8) We could try a hybrid of a serialized and object based backends, where key and namespace of the entries are both
 * serialized in one byte-array.
 * <p>
 * This class was initially based on the {@link java.util.HashMap} implementation of the Android JDK, but is now heavily
 * customized towards the use case of table for state entries.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of value
 */
public class StateTable<K, N, S> implements Iterable<StateEntry<K, N, S>> {

	/**
	 * Min capacity (other than zero) for a {@link StateTable}. Must be a power of two
	 * greater than 1 (and less than 1 << 30).
	 */
	private static final int MINIMUM_CAPACITY = 4;

	/**
	 * Max capacity for a {@link StateTable}. Must be a power of two >= MINIMUM_CAPACITY.
	 */
	private static final int MAXIMUM_CAPACITY = 1 << 30;

	/**
	 * Minimum number of entries that one step of incremental rehashing migrates from the old to the new sub-table.
	 */
	private static final int MIN_TRANSFERRED_PER_INCREMENTAL_REHASH = 16;

	/**
	 * An empty table shared by all zero-capacity maps (typically from default
	 * constructor). It is never written to, and replaced on first put. Its size
	 * is set to half the minimum, so that the first resize will create a
	 * minimum-sized table.
	 */
	private static final StateTableEntry<?, ?, ?>[] EMPTY_TABLE = new StateTableEntry[MINIMUM_CAPACITY >>> 1];

	/**
	 * Empty entry that we use to bootstrap our {@link StateTable.StateEntryIterator}.
	 */
	private static final StateTableEntry<?, ?, ?> ITERATOR_BOOTSTRAP_ENTRY = new StateTableEntry<>();

	/**
	 * The (two) state table(s). On index 0, we always maintain the primary table. On index 1, we sometimes keep a
	 * secondary table. We only have a secondary table during a rehash, which is done incrementally. Size of the array
	 * itself is always 2.
	 */
	private final StateTableEntry<K, N, S>[][] tables;

	/**
	 * The number of mappings in the (two) state table(s). Size of the array is always 2, second index is only used in
	 * case of incremental rehash.
	 */
	private final int[] sizes;

	/**
	 * Combined meta information such as name and serializers for this state
	 */
	private RegisteredBackendStateMetaInfo<N, S> metaInfo;

	/**
	 * The last namespace that was actually inserted. This is a small optimization to reduce duplicate namespace objects.
	 */
	private N lastNamespace;

	/**
	 * The current version of this map. Used for copy-on-write mechanics.
	 */
	private int stateTableVersion;

	/**
	 * The highest version of this map that is still required by any unreleased snapshot.
	 */
	private int highestRequiredSnapshotVersion;

	/**
	 * The next index for a step of incremental rehashing on the main table (tables[0]).
	 */
	private int rehashIndex;

	/**
	 * The {@link StateTable} is rehashed when its size exceeds this threshold.
	 * The value of this field is generally .75 * capacity, except when
	 * the capacity is zero, as described in the EMPTY_TABLE declaration
	 * above.
	 */
	private int threshold;

	/**
	 * Incremented by "structural modifications" to allow (best effort)
	 * detection of concurrent modification.
	 */
	private int modCount;

	/**
	 * Constructs a new {@code StateTable} with default capacity of 128.
	 *
	 * @param metaInfo the meta information, including the type serializer for state copy-on-write.
	 */
	public StateTable(RegisteredBackendStateMetaInfo<N, S> metaInfo) {
		this(128, metaInfo);
	}

	/**
	 * Constructs a new {@code StateTable} instance with the specified capacity.
	 *
	 * @param capacity the initial capacity of this hash map.
	 * @param metaInfo the meta information, including the type serializer for state copy-on-write.
	 * @throws IllegalArgumentException when the capacity is less than zero.
	 */
	@SuppressWarnings("unchecked")
	public StateTable(int capacity, RegisteredBackendStateMetaInfo<N, S> metaInfo) {

		this.metaInfo = Preconditions.checkNotNull(metaInfo);

		//size 2, initialized to EMPTY_TABLE.
		this.tables = new StateTableEntry[][]{EMPTY_TABLE, EMPTY_TABLE};

		//size 2, initialized to 0.
		this.sizes = new int[2];

		this.rehashIndex = 0;
		this.stateTableVersion = 0;
		this.highestRequiredSnapshotVersion = stateTableVersion;

		if (capacity < 0) {
			throw new IllegalArgumentException("Capacity: " + capacity);
		}

		if (capacity == 0) {
			threshold = -1;
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

	// Main interface methods of StateTable ----------------------------------------------------------------------------

	/**
	 * Returns whether this {@link StateTable} is empty.
	 *
	 * @return {@code true} if this {@link StateTable} has no elements, {@code false}
	 * otherwise.
	 * @see #size()
	 */
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Returns the total number of entries in this {@link StateTable}. This is the sum of both sub-tables.
	 *
	 * @return the number of entries in this {@link StateTable}.
	 */
	public int size() {
		return sizes[0] + sizes[1];
	}

	/**
	 * Returns the value of the mapping with the specified key/namespace composite key.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @return the value of the mapping with the specified key/namespace composite key, or {@code null}
	 * if no mapping for the specified key is found.
	 */
	public S get(Object key, Object namespace) {

		assert (null != key && null != namespace);

		incrementalRehash();

		final int requiredVersion = highestRequiredSnapshotVersion;
		final int hash = compositeHash(key, namespace);
		StateTableEntry<K, N, S>[] tab = tables[selectActiveTable(hash)];
		int index = hash & (tab.length - 1);

		for (StateTableEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			final K eKey = e.key;
			final N eNamespace = e.namespace;
			if ((e.hash == hash && key.equals(eKey) && namespace.equals(eNamespace))) {

				// copy-on-write check for state
				if (e.stateVersion < requiredVersion) {
					// copy-on-write check for entry
					if (e.entryVersion < requiredVersion) {
						e = handleChainedEntryCopyOnWrite(tab, hash & (tab.length - 1), e);
					}
					e.stateVersion = stateTableVersion;
					e.state = getStateSerializer().copy(e.state);
				}

				return e.state;
			}
		}

		return null;
	}

	/**
	 * Returns whether this table contains the specified key/namespace composite key.
	 *
	 * @param key       the key in the composite key to search for. Not null.
	 * @param namespace the namespace in the composite key to search for. Not null.
	 * @return {@code true} if this map contains the specified key/namespace composite key,
	 * {@code false} otherwise.
	 */
	public boolean containsKey(Object key, Object namespace) {

		assert (null != key && null != namespace);

		incrementalRehash();

		final int hash = compositeHash(key, namespace);
		StateTableEntry<K, N, S>[] tab = tables[selectActiveTable(hash)];
		int index = hash & (tab.length - 1);

		for (StateTableEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			final K eKey = e.key;
			final N eNamespace = e.namespace;

			if ((e.hash == hash && key.equals(eKey) && namespace.equals(eNamespace))) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Maps the specified key/namespace composite key to the specified value. This method should be preferred
	 * over {@link #putAndGetOld(Object, Object, Object)} (Object, Object)} when the caller is not interested
	 * in the old value, because this can potentially reduce copy-on-write activity.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @param value     the value. Can be null.
	 */
	public void put(K key, N namespace, S value) {

		assert (null != key && null != namespace);

		incrementalRehash();

		final int hash = compositeHash(key, namespace);
		final int insertTableIdx = selectActiveTable(hash);
		StateTableEntry<K, N, S>[] tab = tables[insertTableIdx];
		int index = hash & (tab.length - 1);

		final int requiredVersion = highestRequiredSnapshotVersion;

		for (StateTableEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {

				// copy-on-write check for entry
				if (e.entryVersion < requiredVersion) {
					e = handleChainedEntryCopyOnWrite(tab, index, e);
				}

				e.state = value;
				e.stateVersion = stateTableVersion;
				return;
			}
		}

		++modCount;
		if (size() > threshold) {
			doubleCapacity();
		}
		addNewStateTableEntry(insertTableIdx, key, namespace, value, hash);
	}

	/**
	 * Maps the specified key/namespace composite key to the specified value. Returns the previous state that was
	 * registered under the composite key.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @param value     the value. Can be null.
	 * @return the value of any previous mapping with the specified key or
	 * {@code null} if there was no such mapping.
	 */
	public S putAndGetOld(K key, N namespace, S value) {

		assert (null != key && null != namespace);

		incrementalRehash();

		final int hash = compositeHash(key, namespace);
		final int insertTableIdx = selectActiveTable(hash);
		StateTableEntry<K, N, S>[] tab = tables[insertTableIdx];
		int index = hash & (tab.length - 1);

		final int requiredVersion = highestRequiredSnapshotVersion;

		for (StateTableEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {

				S oldState;

				// copy-on-write check for entry
				if (e.entryVersion < requiredVersion) {
					e = handleChainedEntryCopyOnWrite(tab, index, e);
				}

				// copy-on-write check for state
				if (e.stateVersion < requiredVersion) {
					oldState = getStateSerializer().copy(e.state);
					e.stateVersion = stateTableVersion;
				} else {
					oldState = e.state;
				}

				e.state = value;
				return oldState;
			}
		}

		++modCount;
		if (size() > threshold) {
			doubleCapacity();
		}

		addNewStateTableEntry(insertTableIdx, key, namespace, value, hash);
		return null;
	}

	/**
	 * Removes the mapping with the specified key/namespace composite key from this map. This method should be preferred
	 * over {@link #removeAndGetOld(Object, Object)} when the caller is not interested in the old value, because this
	 * can potentially reduce copy-on-write activity.
	 *
	 * @param key       the key of the mapping to remove. Not null.
	 * @param namespace the namespace of the mapping to remove. Not null.
	 */
	public void remove(Object key, Object namespace) {

		assert (null != key && null != namespace);

		incrementalRehash();

		final int hash = compositeHash(key, namespace);
		final int removeTableIdx = selectActiveTable(hash);
		StateTableEntry<K, N, S>[] tab = tables[removeTableIdx];
		int index = hash & (tab.length - 1);

		for (StateTableEntry<K, N, S> e = tab[index], prev = null; e != null; prev = e, e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {
				if (prev == null) {
					tab[index] = e.next;
				} else {
					// copy-on-write check for entry
					if (prev.entryVersion < highestRequiredSnapshotVersion) {
						prev = handleChainedEntryCopyOnWrite(tab, index, prev);
					}
					prev.next = e.next;
				}
				++modCount;
				--sizes[removeTableIdx];
				return;
			}
		}
	}

	/**
	 * Removes the mapping with the specified key/namespace composite key from this map, returning the state that was
	 * found under the entry.
	 *
	 * @param key       the key of the mapping to remove. Not null.
	 * @param namespace the namespace of the mapping to remove. Not null.
	 * @return the value of the removed mapping or {@code null} if no mapping
	 * for the specified key was found.
	 */
	public S removeAndGetOld(Object key, Object namespace) {

		assert (null != key && null != namespace);

		incrementalRehash();

		final int hash = compositeHash(key, namespace);
		final int removeTableIdx = selectActiveTable(hash);
		StateTableEntry<K, N, S>[] tab = tables[removeTableIdx];
		int index = hash & (tab.length - 1);

		final int requiredVersion = highestRequiredSnapshotVersion;

		for (StateTableEntry<K, N, S> e = tab[index], prev = null; e != null; prev = e, e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {
				if (prev == null) {
					tab[index] = e.next;
				} else {
					// copy-on-write check for entry
					if (prev.entryVersion < requiredVersion) {
						prev = handleChainedEntryCopyOnWrite(tab, index, prev);
					}
					prev.next = e.next;
				}
				++modCount;
				--sizes[removeTableIdx];
				// copy-on-write check for state
				return e.stateVersion < requiredVersion ? getStateSerializer().copy(e.state) : e.state;
			}
		}

		return null;
	}

	// Meta data setter / getter and toString --------------------------------------------------------------------------

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

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("VersionedHashMap{");
		boolean separatorRequired = false;
		for (int i = 0; i <= 1; ++i) {
			StateTableEntry<K, N, S>[] tab = tables[i];
			for (StateTableEntry<K, N, S> entry : tab) {
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

	// Iteration  ------------------------------------------------------------------------------------------------------

	@Override
	public Iterator<StateEntry<K, N, S>> iterator() {
		return new StateEntryIterator();
	}

	// Snapshotting ----------------------------------------------------------------------------------------------------

	/**
	 * Creates a snapshot of this {@link StateTable}, to be written in checkpointing. The snapshot integrity is
	 * protected through copy-on-write from the {@link StateTable}. Users should call
	 * {@link #releaseSnapshot(StateTableSnapshot)} after using the returned object.
	 *
	 * @param keySerializer  the key serializer.
	 * @param keyGroupRange  the key-group range which is {@link StateTable} is responsible for.
	 * @param totalKeyGroups the total number of key-groups in the current job.
	 * @return a snapshot from this {@link StateTable}, for checkpointing.
	 */
	public StateTableSnapshot<K, N, S> createSnapshot(
			TypeSerializer<K> keySerializer,
			KeyGroupRange keyGroupRange,
			int totalKeyGroups) {

		return new StateTableSnapshot<>(
				snapshotTableArrays(),
				size(),
				keySerializer,
				metaInfo.getNamespaceSerializer(),
				metaInfo.getStateSerializer(),
				keyGroupRange,
				totalKeyGroups,
				stateTableVersion);
	}

	/**
	 * Releases a snapshot for this {@link StateTable}. This method should be called once a snapshot is no more needed,
	 * so that the {@link StateTable} can stop considering this snapshot for copy-on-write, thus avoiding unnecessary
	 * object creation.
	 *
	 * @param snapshotToRelease the snapshot to release.
	 */
	public void releaseSnapshot(StateTableSnapshot<K, N, S> snapshotToRelease) {
		releaseSnapshot(snapshotToRelease.getSnapshotVersion());
	}

	// Private utility functions for StateTable management -------------------------------------------------------------

	/**
	 * @see #releaseSnapshot(StateTableSnapshot)
	 */
	public synchronized void releaseSnapshot(int snapshotVersion) {

		// we guard against concurrent modifications of highestRequiredSnapshotVersion between snapshot and release.
		// Only stale reads of from the result of #releaseSnapshot calls are ok.
		assert Thread.holdsLock(this);

		if (snapshotVersion == highestRequiredSnapshotVersion) {
			highestRequiredSnapshotVersion = 0;
		}
	}

	/**
	 * Creates (combined) copy of the table arrays for a snapshot. This method must be called by the same Thread that
	 * does modifications to the {@link StateTable}.
	 */
	@VisibleForTesting
	@SuppressWarnings("unchecked")
	synchronized StateTableEntry<K, N, S>[] snapshotTableArrays() {

		// we guard against concurrent modifications of highestRequiredSnapshotVersion between snapshot and release.
		// Only stale reads of from the result of #releaseSnapshot calls are ok. This is why we must call this method
		// from the same thread that does all the modifications to the table.
		assert Thread.holdsLock(this);

		// increase the table version for copy-on-write and register the snapshot
		++stateTableVersion;
		highestRequiredSnapshotVersion = stateTableVersion;

		StateTableEntry<K, N, S>[] table = tables[0];
		if (isRehashing()) {
			// consider both tables for the snapshot, the rehash index tells us which part of the two tables we need
			final int rhIdx = rehashIndex;
			final int len = table.length - rhIdx;
			StateTableEntry<K, N, S>[] copy = new StateTableEntry[rhIdx + table.length];
			// for the primary table, take every index >= rhIdx.
			System.arraycopy(table, rhIdx, copy, 0, len);

			// for the new table, we are sure that two regions contain all the entries:
			// (0, rhIdx( AND (table.length / 2, table.length / 2 + rhIdx(
			table = tables[1];
			System.arraycopy(table, 0, copy, len, rhIdx);
			System.arraycopy(table, table.length >>> 1, copy, len + rhIdx, rhIdx);

			return copy;
		} else {
			// we only need to copy the primary table
			return Arrays.copyOf(table, table.length);
		}
	}

	/**
	 * Allocate a table of the given capacity and set the threshold accordingly.
	 *
	 * @param newCapacity must be a power of two
	 */
	private void makeTable(int newCapacity, int tableIdx) {
		@SuppressWarnings("unchecked") StateTableEntry<K, N, S>[] newTable
				= (StateTableEntry<K, N, S>[]) new StateTableEntry[newCapacity];
		tables[tableIdx] = newTable;
		threshold = (newCapacity >> 1) + (newCapacity >> 2); // 3/4 capacity
	}

	/**
	 * Creates and inserts a new {@link StateTableEntry}.
	 */
	private void addNewStateTableEntry(int subTableIndex, K key, N namespace, S value, int hash) {

		// small optimization that aims to avoid holding references on duplicate namespace objects
		if (namespace.equals(lastNamespace)) {
			namespace = lastNamespace;
		} else {
			lastNamespace = namespace;
		}

		StateTableEntry<K, N, S>[] insertTable = tables[subTableIndex];
		int index = hash & (insertTable.length - 1);
		insertTable[index] = new StateTableEntry<>(
				key,
				namespace,
				value,
				hash,
				insertTable[index],
				stateTableVersion,
				stateTableVersion);

		++sizes[subTableIndex];
	}

	/**
	 * Select the sub-table which is responsible for entries with the given hash code.
	 *
	 * @param hashCode the hash code which we use to decide about the table that is responsible.
	 * @return the index of the sub-table that is responsible for the entry with the given hash code.
	 */
	private int selectActiveTable(int hashCode) {
		return (hashCode & (tables[0].length - 1)) >= rehashIndex ? 0 : 1;
	}

	/**
	 * Doubles the capacity of the hash table. Existing entries are placed in
	 * the correct bucket on the enlarged table. If the current capacity is,
	 * MAXIMUM_CAPACITY, this method is a no-op. Returns the table, which
	 * will be new unless we were already at MAXIMUM_CAPACITY.
	 */
	private void doubleCapacity() {

		// There can only be one rehash in flight. From the amount of incremental rehash steps we take, this should always hold.
		Preconditions.checkState(!isRehashing(), "There is already a rehash in progress.");

		StateTableEntry<K, N, S>[] oldTable = tables[0];

		int oldCapacity = oldTable.length;

		if (oldCapacity == MAXIMUM_CAPACITY) {
			return;
		}

		int newCapacity = oldCapacity * 2;
		makeTable(newCapacity, 1);
	}

	/**
	 * Returns true, if an incremental rehash is in progress.
	 */
	@VisibleForTesting
	boolean isRehashing() {
		// if we rehash, the secondary table is not empty
		return tables[1] != EMPTY_TABLE;
	}

	/**
	 * Runs a number of steps for incremental rehashing.
	 */
	@SuppressWarnings("unchecked")
	private void incrementalRehash() {

		if (!isRehashing()) {
			return;
		}

		StateTableEntry<K, N, S>[] oldTable = tables[0];
		StateTableEntry<K, N, S>[] newTable = tables[1];

		int oldCapacity = oldTable.length;
		int newMask = newTable.length - 1;
		int requiredVersion = highestRequiredSnapshotVersion;
		int rhIdx = rehashIndex;
		int transferred = 0;

		// we move a certain minimum amount of entries from the old to the new table
		while (transferred < MIN_TRANSFERRED_PER_INCREMENTAL_REHASH) {

			StateTableEntry<K, N, S> e = oldTable[rhIdx];

			while (e != null) {
				// copy-on-write check for entry
				if (e.entryVersion < requiredVersion) {
					e = new StateTableEntry<>(e, stateTableVersion);
				}
				StateTableEntry<K, N, S> n = e.next;
				int pos = e.hash & newMask;
				e.next = newTable[pos];
				newTable[pos] = e;
				e = n;
				++transferred;
			}

			oldTable[rhIdx] = null;
			if (++rhIdx == oldCapacity) {
				//here, the rehash complete and we release resources and reset fields
				tables[0] = newTable;
				tables[1] = (StateTableEntry<K, N, S>[]) EMPTY_TABLE;
				sizes[0] += sizes[1];
				sizes[1] = 0;
				rehashIndex = 0;
				return;
			}
		}

		// sync our local bookkeeping with official the bookkeeping fields
		sizes[0] -= transferred;
		sizes[1] += transferred;
		rehashIndex = rhIdx;
	}

	/**
	 * Perform copy-on-write for entry chains. We iterate the (hopefully and probably) still cached chain, replace
	 * all links up to the 'untilEntry', which we actually wanted to modify.
	 */
	private StateTableEntry<K, N, S> handleChainedEntryCopyOnWrite(
			StateTableEntry<K, N, S>[] tab,
			int tableIdx,
			StateTableEntry<K, N, S> untilEntry) {

		final int required = highestRequiredSnapshotVersion;

		StateTableEntry<K, N, S> current = tab[tableIdx];
		StateTableEntry<K, N, S> copy;

		if (current.entryVersion < required) {
			copy = new StateTableEntry<>(current, stateTableVersion);
			tab[tableIdx] = copy;
		} else {
			// nothing to do, just advance copy to current
			copy = current;
		}

		// we iterate the chain up to 'until entry'
		while (current != untilEntry) {

			//advance current
			current = current.next;

			if (current.entryVersion < required) {
				// copy and advance the current's copy
				copy.next = new StateTableEntry<>(current, stateTableVersion);
				copy = copy.next;
			} else {
				// nothing to do, just advance copy to current
				copy = current;
			}
		}

		return copy;
	}

	@SuppressWarnings("unchecked")
	private static <K, N, S> StateTableEntry<K, N, S> getBootstrapEntry() {
		return (StateTableEntry<K, N, S>) ITERATOR_BOOTSTRAP_ENTRY;
	}

	/**
	 * Helper function that creates and scrambles a composite hash for key and namespace.
	 */
	private static int compositeHash(Object key, Object namespace) {

		// composite by XOR
		int compositeHash = (key.hashCode() ^ namespace.hashCode());

		// some bit-mixing to guard against bad hash functions (from Murmur's 32 bit finalizer)
		compositeHash ^= compositeHash >>> 16;
		compositeHash *= 0x85ebca6b;
		compositeHash ^= compositeHash >>> 13;
		compositeHash *= 0xc2b2ae35;
		compositeHash ^= compositeHash >>> 16;
		return compositeHash;
	}

	// StateTableEntry -------------------------------------------------------------------------------------------------

	/**
	 * One entry in the {@link StateTable}. This is a triplet of key, namespace, and state. Thereby, key and namespace
	 * together serve as a composite key for the state. This class also contains some management meta data for
	 * copy-on-write, a pointer to link other {@link StateTableEntry}s to a list, and cached hash code.
	 *
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 * @param <S> type of state.
	 */
	static class StateTableEntry<K, N, S> implements StateEntry<K, N, S> {

		/**
		 * The key. Assumed to be immutable and not null.
		 */
		final K key;

		/**
		 * The namespace. Assumed to be immutable and not null.
		 */
		final N namespace;

		/**
		 * The computed secondary hash for the composite of key and namespace.
		 */
		final int hash;

		/**
		 * The state. This is not final to allow exchanging the object for copy-on-write. Can be null.
		 */
		S state;

		/**
		 * Link to another {@link StateTableEntry}. This is used to resolve collisions in the {@link StateTable} through chaining.
		 */
		StateTableEntry<K, N, S> next;

		/**
		 * The version of this {@link StateTableEntry}. This is meta data for copy-on-write of the table structure.
		 */
		int entryVersion;

		/**
		 * The version of the state object in this entry. This is meta data for copy-on-write of the state object itself.
		 */
		int stateVersion;

		StateTableEntry() {
			this(null, null, null, 0, null, 0, 0);
		}

		StateTableEntry(StateTableEntry<K, N, S> other, int entryVersion) {
			this(other.key, other.namespace, other.state, other.hash, other.next, entryVersion, other.stateVersion);
		}

		StateTableEntry(
				K key,
				N namespace,
				S state,
				int hash,
				StateTableEntry<K, N, S> next,
				int entryVersion,
				int stateVersion) {

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

		@Override
		public final S getState() {
			return state;
		}

		public final void setState(S value, int mapVersion) {
			// naturally, we can update the state version every time we replace the old state with a different object
			if (value != state) {
				this.state = value;
				this.stateVersion = mapVersion;
			}
		}

		@Override
		public final boolean equals(Object o) {
			if (!(o instanceof StateTable.StateTableEntry)) {
				return false;
			}

			StateTableEntry<?, ?, ?> e = (StateTableEntry<?, ?, ?>) o;
			return e.getKey().equals(key)
					&& e.getNamespace().equals(namespace)
					&& Objects.equals(e.getState(), state);
		}

		@Override
		public final int hashCode() {
			return (key.hashCode() ^ namespace.hashCode()) ^ Objects.hashCode(state);
		}

		@Override
		public final String toString() {
			return "(" + key + "|" + namespace + ")=" + state;
		}
	}

	// StateEntryIterator  ---------------------------------------------------------------------------------------------

	/**
	 * Iterator over the entries in a {@link StateTable}.
	 */
	class StateEntryIterator implements Iterator<StateEntry<K, N, S>> {
		private int activeSubTableId;
		private int nextTablePosition;
		private StateTableEntry<K, N, S> nextEntry;
		private int expectedModCount = modCount;

		StateEntryIterator() {
			this.activeSubTableId = 0;
			this.nextTablePosition = 0;
			this.expectedModCount = modCount;
			this.nextEntry = getBootstrapEntry();
			advanceIterator();
		}

		private StateTableEntry<K, N, S> advanceIterator() {

			StateTableEntry<K, N, S> entryToReturn = nextEntry;
			StateTableEntry<K, N, S> next = entryToReturn.next;

			// consider both sub-tables tables to cover the case of rehash
			while (next == null && activeSubTableId <= 1) {

				StateTableEntry<K, N, S>[] tab = tables[activeSubTableId];

				while (nextTablePosition < tab.length) {
					next = tab[nextTablePosition++];

					if (next != null) {
						nextEntry = next;
						return entryToReturn;
					}
				}

				nextTablePosition = 0;
				++activeSubTableId;
			}

			nextEntry = next;
			return entryToReturn;
		}

		@Override
		public boolean hasNext() {
			return nextEntry != null;
		}

		@Override
		public StateTableEntry<K, N, S> next() {
			if (modCount != expectedModCount) {
				throw new ConcurrentModificationException();
			}
			if (nextEntry == null) {
				throw new NoSuchElementException();
			}

			return advanceIterator();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Read-only iterator");
		}
	}
}
