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
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * This class represents the snapshot of a {@link StateTable} and has a role in operator state checkpointing. Besides
 * holding the {@link StateTable}s internal entries at the time of the snapshot, this class is also responsible for
 * preparing and writing the state in the process of checkpointing.
 * <p>
 * IMPORTANT: Please notice that snapshot integrity of entries in this class rely on proper copy-on-write semantics
 * through the {@link StateTable} that created the snapshot object, but all objects in this snapshot must be considered
 * as READ-ONLY!. The reason is that the objects held by this class may or may not be deep copies of original objects
 * that may still used in the {@link StateTable}. This depends for each entry on whether or not it was subject to
 * copy-on-write operations by the {@link StateTable}. Phrased differently: the {@link StateTable} provides
 * copy-on-write isolation for this snapshot, but this snapshot does not isolate modifications from the
 * {@link StateTable}!
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class StateTableSnapshot<K, N, S> {

	/**
	 * Serializer for the key
	 */
	private final TypeSerializer<K> keySerializer;

	/**
	 * Serializer for the namespace
	 */
	private final TypeSerializer<N> namespaceSerializer;

	/**
	 * Serializer for the state
	 */
	private final TypeSerializer<S> stateSerializer;

	/**
	 * The key group range for which the {@link StateTable} which created this snapshot was responsible.
	 */
	private final KeyGroupRange keyGroupRange;

	/**
	 * The total number of key groups in the job in which this snapshot was created.
	 */
	private final int totalNumberOfKeyGroups;

	/**
	 * Version of the {@link StateTable} when this snapshot was created. This can be used to release the snapshot.
	 */
	private final int snapshotVersion;

	/**
	 * The state table entries, as by the time this snapshot was created. Objects in this array may or may not be deep
	 * copies of the current entries in the {@link StateTable} that created this snapshot. This depends for each entry
	 * on whether or not it was subject to copy-on-write operations by the {@link StateTable}.
	 */
	private StateTable.StateTableEntry<K, N, S>[] snapshotData;

	/**
	 * Offsets for the individual key-groups. This is lazily created when the snapshot is grouped by key-group during
	 * the process of writing this snapshot to an output as part of checkpointing.
	 */
	private int[] keyGroupOffsets;

	/**
	 * The number of entries in the {@link StateTable} at the time of creating this snapshot.
	 */
	private int stateTableSize;

	public StateTableSnapshot(
			StateTable.StateTableEntry<K, N, S>[] ungroupedDump,
			int stateTableSize,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<S> stateSerializer,
			KeyGroupRange keyGroupRange,
			int totalNumberOfKeyGroups,
			int snapshotVersion) {

		this.snapshotData = Preconditions.checkNotNull(ungroupedDump);
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
		this.stateSerializer = Preconditions.checkNotNull(stateSerializer);
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
		this.totalNumberOfKeyGroups = totalNumberOfKeyGroups;
		this.stateTableSize = stateTableSize;
		this.keyGroupOffsets = null;
		this.snapshotVersion = snapshotVersion;
	}

	/**
	 * Returns the internal version of the {@link StateTable} when this snapshot was created. This value must be used to
	 * tell the {@link StateTable} when to release this snapshot.
	 */
	public int getSnapshotVersion() {
		return snapshotVersion;
	}

	/**
	 * Partitions the snapshot data by key-group. The algorithm first builds a histogram for the distribution of keys
	 * into key-groups. Then, the histogram is accumulated to obtain the boundaries of each key-group in an array.
	 * Last, we use the accumulated counts as write position pointers for the key-group's bins when reordering the
	 * entries by key-group. This operation is lazily performed before the first writing of a key-group.
	 * <p>
	 * As a possible future optimization, we could perform the repartitioning in-place, using a scheme similar to the
	 * cuckoo cycles in cuckoo hashing. This can trade some performance for a smaller memory footprint.
	 */
	@SuppressWarnings("unchecked")
	private void partitionEntriesByKeyGroup() {

		// We only have to perform this step once before the first key-group is written
		if (null != keyGroupOffsets) {
			return;
		}

		final int totalKeyGroups = totalNumberOfKeyGroups;
		final int baseKgIdx = keyGroupRange.getStartKeyGroup();
		final int[] histogram = new int[keyGroupRange.getNumberOfKeyGroups() + 1];

		StateTable.StateTableEntry<K, N, S>[] unfold = new StateTable.StateTableEntry[stateTableSize];

		// 1) In this step we i) 'unfold' the linked list of entries to a flat array and ii) build a histogram for key-groups
		int unfoldIndex = 0;
		for (StateTable.StateTableEntry<K, N, S> entry : snapshotData) {
			while (null != entry) {
				int effectiveKgIdx =
						KeyGroupRangeAssignment.computeKeyGroupForKeyHash(entry.key.hashCode(), totalKeyGroups) - baseKgIdx + 1;
				++histogram[effectiveKgIdx];
				unfold[unfoldIndex++] = entry;
				entry = entry.next;
			}
		}

		StateTable.StateTableEntry<K, N, S>[] groupedOut = snapshotData;

		// 2) We accumulate the histogram bins to obtain key-group ranges in the final array
		for (int i = 1; i < histogram.length; ++i) {
			histogram[i] += histogram[i - 1];
		}

		// 3) We repartition the entries by key-group, using the histogram values as write indexes
		for (StateTable.StateTableEntry<K, N, S> t : unfold) {
			int effectiveKgIdx =
					KeyGroupRangeAssignment.computeKeyGroupForKeyHash(t.key.hashCode(), totalKeyGroups) - baseKgIdx;
			groupedOut[histogram[effectiveKgIdx]++] = t;
		}

		// 4) As byproduct, we also created the key-group offsets
		this.keyGroupOffsets = histogram;
	}

	/**
	 * Writes the data for the specified key-group to the output.
	 *
	 * @param dov the output
	 * @param keyGroupId the key-group to write
	 * @throws IOException on write related problems
	 */
	public void writeKeyGroupData(DataOutputView dov, int keyGroupId) throws IOException {

		if (null == keyGroupOffsets) {
			partitionEntriesByKeyGroup();
		}

		final StateTable.StateTableEntry<K, N, S>[] groupedOut = snapshotData;

		int keyGroupOffsetIdx = keyGroupId - keyGroupRange.getStartKeyGroup() - 1;
		int startOffset = keyGroupOffsetIdx < 0 ? 0 : keyGroupOffsets[keyGroupOffsetIdx];
		int endOffset = keyGroupOffsets[keyGroupOffsetIdx + 1];

		// write number of elements
		dov.writeInt(endOffset - startOffset);

		// write elements
		for (int i = startOffset; i < endOffset; ++i) {
			StateTable.StateTableEntry<K, N, S> toWrite = groupedOut[i];
			groupedOut[i] = null; // free asap for GC
			keySerializer.serialize(toWrite.key, dov);
			namespaceSerializer.serialize(toWrite.namespace, dov);
			stateSerializer.serialize(toWrite.state, dov);
		}
	}
}