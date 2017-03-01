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

package org.apache.flink.migration.runtime.state.filesystem;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.migration.runtime.state.KvStateSnapshot;
import org.apache.flink.migration.runtime.state.memory.MigrationRestoreSnapshot;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.AbstractStateTable;

import java.io.IOException;

/**
 * A snapshot of a heap key/value state stored in a file.
 * 
 * @param <K> The type of the key in the snapshot state.
 * @param <N> The type of the namespace in the snapshot state.
 * @param <SV> The type of the state value.
 */
@Deprecated
public abstract class AbstractFsStateSnapshot<K, N, SV, S extends State, SD extends StateDescriptor<S, ?>> 
		extends AbstractFileStateHandle implements KvStateSnapshot<K, N, S, SD>, MigrationRestoreSnapshot<K, N, SV> {

	private static final long serialVersionUID = 1L;

	/** Key Serializer */
	protected final TypeSerializer<K> keySerializer;

	/** Namespace Serializer */
	protected final TypeSerializer<N> namespaceSerializer;

	/** Serializer for the state value */
	protected final TypeSerializer<SV> stateSerializer;

	/** StateDescriptor, for sanity checks */
	protected final SD stateDesc;

	public AbstractFsStateSnapshot(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer,
		SD stateDesc,
		Path filePath) {
		super(filePath);
		this.stateDesc = stateDesc;
		this.keySerializer = keySerializer;
		this.stateSerializer = stateSerializer;
		this.namespaceSerializer = namespaceSerializer;

	}

	@Override
	public long getStateSize() throws IOException {
		return getFileSize();
	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	public TypeSerializer<SV> getStateSerializer() {
		return stateSerializer;
	}

	public SD getStateDesc() {
		return stateDesc;
	}

	@Override
	@SuppressWarnings("unchecked")
	public AbstractStateTable<K, N, SV> deserialize(
			String stateName,
			KeyGroupRange keyGroupRange,
			int totalNumberOfKeyGroups,
			boolean async) throws IOException {

		throw  new UnsupportedOperationException("TODO");

//		Preconditions.checkNotNull(stateName, "State name is null. Cannot deserialize snapshot.");
//		Preconditions.checkNotNull(stateName, "KeyGroupRange is null. Cannot deserialize snapshot.");
//		FileSystem fs = getFilePath().getFileSystem();
//		//TODO register closeable to support fast cancelation?
//		try (FSDataInputStream inStream = fs.open(getFilePath())) {
//
//			DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(inStream);
//
//			final int numNamespaces = inView.readInt();
//			TypeSerializer<K> keySerializer = getKeySerializer();
//			TypeSerializer<N> namespaceSerializer = getNamespaceSerializer();
//			TypeSerializer<SV> stateSerializer = getStateSerializer();
//
//			TypeSerializer<N> patchedNamespaceSerializer = this.namespaceSerializer;
//			if (patchedNamespaceSerializer instanceof VoidSerializer) {
//
//				patchedNamespaceSerializer = (TypeSerializer<N>) VoidNamespaceSerializer.INSTANCE;
//			}
//
//			RegisteredBackendStateMetaInfo<N, SV> registeredBackendStateMetaInfo =
//					new RegisteredBackendStateMetaInfo<>(
//							StateDescriptor.Type.UNKNOWN,
//							stateName,
//							patchedNamespaceSerializer,
//							stateSerializer);
//
//			CopyOnWriteStateTable<K, N, SV> stateMap = new CopyOnWriteStateTable<>(registeredBackendStateMetaInfo);
//
//			for (int i = 0; i < numNamespaces; i++) {
//
//				N namespace = namespaceSerializer.deserialize(inView);
//
//				if (null == namespace) {
//					namespace = (N) VoidNamespace.INSTANCE;
//				}
//
//				final int numKV = inView.readInt();
//				for (int j = 0; j < numKV; j++) {
//					K key = keySerializer.deserialize(inView);
//					SV value = stateSerializer.deserialize(inView);
//					stateMap.put(key, namespace, value);
//				}
//			}
//			return stateMap;
//		} catch (Exception e) {
//			throw new IOException("Failed to restore state from file system", e);
//		}
	}
}
