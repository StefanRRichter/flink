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

package org.apache.flink.streaming.util;

import org.apache.flink.runtime.checkpoint.savepoint.SavepointV1Serializer;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.snapshot.KeyedStateHandleSnapshot;
import org.apache.flink.runtime.state.snapshot.OperatorStateHandleSnapshot;
import org.apache.flink.runtime.state.snapshot.OperatorSubtaskStateReport;
import org.apache.flink.runtime.state.snapshot.SnapshotMetaData;
import org.apache.flink.runtime.state.snapshot.SnapshotUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Util for writing/reading {@link org.apache.flink.streaming.runtime.tasks.OperatorStateHandles},
 * for use in tests.
 */
public class OperatorSnapshotUtil {

	public static String getResourceFilename(String filename) {
		ClassLoader cl = OperatorSnapshotUtil.class.getClassLoader();
		URL resource = cl.getResource(filename);
		return resource.getFile();
	}

	public static void writeStateHandle(OperatorSubtaskStateReport state, String path) throws IOException {
		FileOutputStream out = new FileOutputStream(path);

		try (DataOutputStream dos = new DataOutputStream(out)) {

			// still required for compatibility
			dos.writeInt(0);

			// still required for compatibility
			SavepointV1Serializer.serializeStreamStateHandle(null, dos);

			Collection<OperatorStateHandle> rawOperatorState =
				SnapshotUtils.findPrimarySnapshotHandles(state.getRawOperatorState());

			if (rawOperatorState != null) {
				dos.writeInt(rawOperatorState.size());
				for (OperatorStateHandle operatorStateHandle : rawOperatorState) {
					SavepointV1Serializer.serializeOperatorStateHandle(operatorStateHandle, dos);
				}
			} else {
				// this means no states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<OperatorStateHandle> managedOperatorState =
				SnapshotUtils.findPrimarySnapshotHandles(state.getManagedOperatorState());

			if (managedOperatorState != null) {
				dos.writeInt(managedOperatorState.size());
				for (OperatorStateHandle operatorStateHandle : managedOperatorState) {
					SavepointV1Serializer.serializeOperatorStateHandle(operatorStateHandle, dos);
				}
			} else {
				// this means no states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<KeyedStateHandle> rawKeyedState =
				SnapshotUtils.findPrimarySnapshotHandles(state.getRawKeyedState());

			if (rawKeyedState != null) {
				dos.writeInt(rawKeyedState.size());
				for (KeyedStateHandle keyedStateHandle : rawKeyedState) {
					SavepointV1Serializer.serializeKeyedStateHandle(keyedStateHandle, dos);
				}
			} else {
				// this means no operator states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<KeyedStateHandle> managedKeyedState =
				SnapshotUtils.findPrimarySnapshotHandles(state.getManagedKeyedState());

			if (managedKeyedState != null) {
				dos.writeInt(managedKeyedState.size());
				for (KeyedStateHandle keyedStateHandle : managedKeyedState) {
					SavepointV1Serializer.serializeKeyedStateHandle(keyedStateHandle, dos);
				}
			} else {
				// this means no operator states, not even an empty list
				dos.writeInt(-1);
			}

			dos.flush();
		}
	}

	public static OperatorSubtaskStateReport readStateHandle(String path) throws IOException, ClassNotFoundException {
		FileInputStream in = new FileInputStream(path);
		try (DataInputStream dis = new DataInputStream(in)) {
			int index = dis.readInt();

			// still required for compatibility to consume the bytes.
			SavepointV1Serializer.deserializeStreamStateHandle(dis);

			List<OperatorStateHandleSnapshot> rawOperatorState = null;
			int numRawOperatorStates = dis.readInt();
			if (numRawOperatorStates >= 0) {
				rawOperatorState = new ArrayList<>();
				for (int i = 0; i < numRawOperatorStates; i++) {
					OperatorStateHandle operatorState = SavepointV1Serializer.deserializeOperatorStateHandle(dis);
					OperatorStateHandleSnapshot operatorStateSnapshot = operatorState != null ?
						new OperatorStateHandleSnapshot(
							SnapshotMetaData.createPrimarySnapshotMetaData(),
							operatorState)
						: null;
					rawOperatorState.add(operatorStateSnapshot);
				}
			}

			List<OperatorStateHandleSnapshot> managedOperatorState = null;
			int numManagedOperatorStates = dis.readInt();
			if (numManagedOperatorStates >= 0) {
				managedOperatorState = new ArrayList<>();
				for (int i = 0; i < numManagedOperatorStates; i++) {
					OperatorStateHandle operatorState = SavepointV1Serializer.deserializeOperatorStateHandle(dis);
					OperatorStateHandleSnapshot operatorStateSnapshot = operatorState != null ?
						new OperatorStateHandleSnapshot(
							SnapshotMetaData.createPrimarySnapshotMetaData(),
							operatorState)
						: null;
					managedOperatorState.add(operatorStateSnapshot);
				}
			}

			List<KeyedStateHandleSnapshot> rawKeyedState = null;
			int numRawKeyedStates = dis.readInt();
			if (numRawKeyedStates >= 0) {
				rawKeyedState = new ArrayList<>();
				for (int i = 0; i < numRawKeyedStates; i++) {
					KeyedStateHandle keyedState = SavepointV1Serializer.deserializeKeyedStateHandle(dis);
					KeyedStateHandleSnapshot keyedStateSnapshot = keyedState != null ?
						new KeyedStateHandleSnapshot(
							SnapshotMetaData.createPrimarySnapshotMetaData(),
							keyedState)
						: null;
					rawKeyedState.add(keyedStateSnapshot);
				}
			}

			List<KeyedStateHandleSnapshot> managedKeyedState = null;
			int numManagedKeyedStates = dis.readInt();
			if (numManagedKeyedStates >= 0) {
				managedKeyedState = new ArrayList<>();
				for (int i = 0; i < numManagedKeyedStates; i++) {
					KeyedStateHandle keyedState = SavepointV1Serializer.deserializeKeyedStateHandle(dis);
					KeyedStateHandleSnapshot keyedStateSnapshot = keyedState != null ?
						new KeyedStateHandleSnapshot(
							SnapshotMetaData.createPrimarySnapshotMetaData(),
							keyedState)
						: null;
					if (keyedState != null) {
						managedKeyedState.add(keyedStateSnapshot);
					}
				}
			}

			return new OperatorSubtaskStateReport(
				managedOperatorState,
				rawOperatorState,
				managedKeyedState,
				rawKeyedState);
		}
	}
}
