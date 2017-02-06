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
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public class StateTableSnapshot<K, N, S> {

	private Tuple3<K, N, S>[] snapshotData;

	private final KeyGroupRange keyGroupRange;
	private final int totalNumberOfKeyGroups;

	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<N> namespaceSerializer;
	private final TypeSerializer<S> stateSerializer;

	private int[] keyGroupOffsets;

	public StateTableSnapshot(
			Tuple3<K, N, S>[] ungroupedDump,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<S> stateSerializer,
			KeyGroupRange keyGroupRange,
			int totalNumberOfKeyGroups) {

		this.snapshotData = Preconditions.checkNotNull(ungroupedDump);
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
		this.stateSerializer = Preconditions.checkNotNull(stateSerializer);
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
		this.totalNumberOfKeyGroups = totalNumberOfKeyGroups;

		this.keyGroupOffsets = null;
	}

	private void partitionByKeyGroup() {

		if (null != keyGroupOffsets) {
			return;
		}

		final int totalKeyGroups = totalNumberOfKeyGroups;

		Tuple3<K, N, S>[] ungroupedIn = snapshotData;
		Tuple3<K, N, S>[] groupedOut = new Tuple3[ungroupedIn.length];

		int baseKgIdx = keyGroupRange.getStartKeyGroup();
		int[] histogram = new int[keyGroupRange.getNumberOfKeyGroups() + 1];

		for (Tuple3<K, N, S> t : ungroupedIn) {
			int effectiveKgIdx =
					KeyGroupRangeAssignment.computeKeyGroupForKeyHash(t.f0.hashCode(), totalKeyGroups) - baseKgIdx + 1;
			++histogram[effectiveKgIdx];
		}

		for (int i = 1; i < histogram.length; ++i) {
			histogram[i] += histogram[i - 1];
		}

		for (Tuple3<K, N, S> t : ungroupedIn) {
			int effectiveKgIdx =
					KeyGroupRangeAssignment.computeKeyGroupForKeyHash(t.f0.hashCode(), totalKeyGroups) - baseKgIdx;
			groupedOut[histogram[effectiveKgIdx]++] = t;
		}

		this.keyGroupOffsets = histogram;
		this.snapshotData = groupedOut;
	}

	public void writeKeyGroupData(
			DataOutputView dov,
			int keyGroupId) throws IOException {

		if (null == keyGroupOffsets) {
			partitionByKeyGroup();
		}

		int keyGroupOffsetIdx = keyGroupId - keyGroupRange.getStartKeyGroup() - 1;
		int startOffset = keyGroupOffsetIdx < 0 ? 0 : keyGroupOffsets[keyGroupOffsetIdx];
		int endOffset = keyGroupOffsets[keyGroupOffsetIdx + 1];

		// write number of elements
		dov.writeInt(endOffset - startOffset);

		// write elements
		for (int i = startOffset; i < endOffset; ++i) {
			Tuple3<K, N, S> toWrite = snapshotData[i];
			keySerializer.serialize(toWrite.f0, dov);
			namespaceSerializer.serialize(toWrite.f1, dov);
			stateSerializer.serialize(toWrite.f2, dov);
		}
	}
}