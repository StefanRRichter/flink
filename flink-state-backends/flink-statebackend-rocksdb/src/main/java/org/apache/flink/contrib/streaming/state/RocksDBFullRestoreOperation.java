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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredKeyedBackendStateMetaInfo;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.contrib.streaming.state.RocksDBSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.contrib.streaming.state.RocksDBSnapshotUtil.clearMetaDataFollowsFlag;
import static org.apache.flink.contrib.streaming.state.RocksDBSnapshotUtil.hasMetaDataFollowsFlag;

public class RocksDBFullRestoreOperation<K> extends RestoreOperationBase<K> {

//	private final RocksDB db;
//
//	private final ColumnFamilyOptions columnOptions;
//
//	private final WriteOptions writeOptions;
//
//	private final CloseableRegistry cancelStreamRegistry;
//
//	private final KeyGroupRange keyGroupRange;
//
//	private final ClassLoader userCodeClassLoader;
//
//	private final TypeSerializer<K> keySerializer;
//
//	private final Map<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation;
//
//	private final Map<String, RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredKvStateMetaInfos;


	/**
	 * Creates a restore operation object for the given state backend instance.
	 *
	 */
	public RocksDBFullRestoreOperation(RocksDBKeyedStateBackend<K> remove) {

	}

	/**
	 * Restores all key-groups data that is referenced by the passed state handles.
	 *
	 * @param keyedStateHandles List of all key groups state handles that shall be restored.
	 */
	public void doRestore(Collection<KeyedStateHandle> keyedStateHandles)
		throws IOException, StateMigrationException, RocksDBException {

		rocksDBKeyedStateBackend.createDB();

		for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {
			if (keyedStateHandle != null) {

				if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
					throw new IllegalStateException("Unexpected state handle type, " +
						"expected: " + KeyGroupsStateHandle.class +
						", but found: " + keyedStateHandle.getClass());
				}

				restoreKeyGroupsInStateHandle((KeyGroupsStateHandle) keyedStateHandle);
			}
		}
	}

	/**
	 * Restore one key groups state handle.
	 */
	private void restoreKeyGroupsInStateHandle(KeyGroupsStateHandle currentKeyGroupsStateHandle)
		throws IOException, StateMigrationException, RocksDBException {

		FSDataInputStream currentStateHandleInStream = null;

		try {
			currentStateHandleInStream = currentKeyGroupsStateHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(currentStateHandleInStream);

			DataInputView currentStateHandleInView = new DataInputViewStreamWrapper(currentStateHandleInStream);

			KeyedBackendSerializationProxy<K> serializationProxy =
				new KeyedBackendSerializationProxy<>(userCodeClassLoader);

			serializationProxy.read(currentStateHandleInView);

			// check for key serializer compatibility; this also reconfigures the
			// key serializer to be compatible, if it is required and is possible
			if (CompatibilityUtil.resolveCompatibilityResult(
				serializationProxy.getKeySerializer(),
				UnloadableDummyTypeSerializer.class,
				serializationProxy.getKeySerializerConfigSnapshot(),
				keySerializer)
				.isRequiresMigration()) {

				// TODO replace with state migration; note that key hash codes need to remain the same after migration
				throw new StateMigrationException("The new key serializer is not compatible to read previous keys. " +
					"Aborting now since state migration is currently not available");
			}

			StreamCompressionDecorator keygroupStreamCompressionDecorator = serializationProxy.isUsingKeyGroupCompression() ?
				SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;

			final List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredMetaInfos =
				serializationProxy.getStateMetaInfoSnapshots();

			final List<ColumnFamilyHandle> currentStateHandleKVStateColumnFamilies = new ArrayList<>(restoredMetaInfos.size());

			final LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation =
				new LinkedHashMap<>();

			final Map<String, RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredKvStateMetaInfos = new HashMap<>();

			restoreKVStateMetaData(
				restoredMetaInfos,
				currentStateHandleKVStateColumnFamilies,
				kvStateInformation,
				restoredKvStateMetaInfos);

			restoreKVStateData(
				currentKeyGroupsStateHandle,
				currentStateHandleInStream,
				currentStateHandleKVStateColumnFamilies,
				keygroupStreamCompressionDecorator);

		} finally {
			if (cancelStreamRegistry.unregisterCloseable(currentStateHandleInStream)) {
				IOUtils.closeQuietly(currentStateHandleInStream);
			}
		}
	}

	/**
	 * Restore the KV-state / ColumnFamily meta data for all key-groups referenced by the current state handle.
	 */
	private void restoreKVStateMetaData(
		List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredMetaInfos,
		List<ColumnFamilyHandle> currentStateHandleKVStateColumnFamilies,
		LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation,
		Map<String, RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredKvStateMetaInfos) throws RocksDBException {

		for (RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> restoredMetaInfo : restoredMetaInfos) {

			Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> registeredColumn =
				kvStateInformation.get(restoredMetaInfo.getName());

			if (registeredColumn == null) {
				byte[] nameBytes = restoredMetaInfo.getName().getBytes(ConfigConstants.DEFAULT_CHARSET);

				ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
					nameBytes,
					columnOptions);

				RegisteredKeyedBackendStateMetaInfo<?, ?> stateMetaInfo =
					new RegisteredKeyedBackendStateMetaInfo<>(
						restoredMetaInfo.getStateType(),
						restoredMetaInfo.getName(),
						restoredMetaInfo.getNamespaceSerializer(),
						restoredMetaInfo.getStateSerializer());

				restoredKvStateMetaInfos.put(restoredMetaInfo.getName(), restoredMetaInfo);

				ColumnFamilyHandle columnFamily = db.createColumnFamily(columnFamilyDescriptor);

				registeredColumn = new Tuple2<>(columnFamily, stateMetaInfo);
				kvStateInformation.put(stateMetaInfo.getName(), registeredColumn);

			} else {
				// TODO with eager state registration in place, check here for serializer migration strategies
			}
			currentStateHandleKVStateColumnFamilies.add(registeredColumn.f0);
		}
	}

	/**
	 * Restore the KV-state / ColumnFamily data for all key-groups referenced by the current state handle.
	 *
	 * @throws IOException
	 * @throws RocksDBException
	 */
	private void restoreKVStateData(
		KeyGroupsStateHandle currentKeyGroupsStateHandle,
		FSDataInputStream currentStateHandleInStream,
		List<ColumnFamilyHandle> currentStateHandleKVStateColumnFamilies,
		StreamCompressionDecorator keygroupStreamCompressionDecorator) throws IOException, RocksDBException {

		//for all key-groups in the current state handle...
		for (Tuple2<Integer, Long> keyGroupOffset : currentKeyGroupsStateHandle.getGroupRangeOffsets()) {
			int keyGroup = keyGroupOffset.f0;

			// Check that restored key groups all belong to the backend
			Preconditions.checkState(keyGroupRange.contains(keyGroup),
				"The key group must belong to the backend");

			long offset = keyGroupOffset.f1;
			//not empty key-group?
			if (0L != offset) {
				currentStateHandleInStream.seek(offset);
				try (InputStream compressedKgIn = keygroupStreamCompressionDecorator.decorateWithCompression(currentStateHandleInStream)) {
					DataInputViewStreamWrapper compressedKgInputView = new DataInputViewStreamWrapper(compressedKgIn);
					//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
					int kvStateId = compressedKgInputView.readShort();
					ColumnFamilyHandle handle = currentStateHandleKVStateColumnFamilies.get(kvStateId);
					//insert all k/v pairs into DB
					boolean keyGroupHasMoreKeys = true;
					while (keyGroupHasMoreKeys) {
						byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
						byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
						if (hasMetaDataFollowsFlag(key)) {
							//clear the signal bit in the key to make it ready for insertion again
							clearMetaDataFollowsFlag(key);
							db.put(handle, writeOptions, key, value);
							//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
							kvStateId = END_OF_KEY_GROUP_MARK
								& compressedKgInputView.readShort();
							if (END_OF_KEY_GROUP_MARK == kvStateId) {
								keyGroupHasMoreKeys = false;
							} else {
								handle = currentStateHandleKVStateColumnFamilies.get(kvStateId);
							}
						} else {
							db.put(handle, writeOptions, key, value);
						}
					}
				}
			}
		}
	}
}
