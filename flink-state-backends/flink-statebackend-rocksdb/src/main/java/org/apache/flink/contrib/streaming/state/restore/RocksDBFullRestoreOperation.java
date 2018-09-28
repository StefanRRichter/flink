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

package org.apache.flink.contrib.streaming.state.restore;

import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.StateColumnFamilyHandle;
import org.apache.flink.contrib.streaming.state.StateRocksDB;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.local.CloseableRegistryClient;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.clearMetaDataFollowsFlag;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.hasMetaDataFollowsFlag;

/**
 * Encapsulates the process of restoring a RocksDBKeyedStateBackend from a full snapshot.
 */
public class RocksDBFullRestoreOperation<K> {

	/** Current key-groups state handle from which we restore key-groups. */
	private KeyGroupsStateHandle currentKeyGroupsStateHandle;
	/** Current input stream we obtained from currentKeyGroupsStateHandle. */
	private FSDataInputStream currentStateHandleInStream;
	/** Current data input view that wraps currentStateHandleInStream. */
	private DataInputView currentStateHandleInView;
	/** Current list of ColumnFamilyHandles for all column families we restore from currentKeyGroupsStateHandle. */
	private List<ColumnFamilyHandle> currentStateHandleKVStateColumnFamilies;
	/** The compression decorator that was used for writing the state, as determined by the meta data. */
	private StreamCompressionDecorator keygroupStreamCompressionDecorator;

	@Nonnull
	private final StateRocksDB stateRocksDB;

	@Nonnull
	private final CloseableRegistryClient closeableRegistryClient;

	@Nonnull
	private final ClassLoader userCodeClassLoader;

	@Nonnull
	private final TypeSerializer<K> keySerializer;

	@Nonnull
	private final ColumnFamilyOptions columnFamilyOptions;

	@Nonnull
	private final KeyGroupRange backendKeyGroupRange;

	/**
	 * Creates a restore operation object for the given state backend instance.
	 */
	public RocksDBFullRestoreOperation() {

	}

	/**
	 * Restores all key-groups data that is referenced by the passed state handles.
	 *
	 * @param keyedStateHandles List of all key groups state handles that shall be restored.
	 */
	public void doRestore(Collection<KeyedStateHandle> keyedStateHandles)
		throws IOException, StateMigrationException, RocksDBException {

		for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {
			if (keyedStateHandle != null) {

				if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
					throw new IllegalStateException("Unexpected state handle type, " +
						"expected: " + KeyGroupsStateHandle.class +
						", but found: " + keyedStateHandle.getClass());
				}
				this.currentKeyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
				restoreKeyGroupsInStateHandle();
			}
		}
	}

	/**
	 * Restore one key groups state handle.
	 */
	private void restoreKeyGroupsInStateHandle()
		throws IOException, StateMigrationException, RocksDBException {
		try {
			currentStateHandleInStream = currentKeyGroupsStateHandle.openInputStream();
			closeableRegistryClient.registerCloseable(currentStateHandleInStream);
			currentStateHandleInView = new DataInputViewStreamWrapper(currentStateHandleInStream);
			restoreKVStateMetaData();
			restoreKVStateData();
		} finally {
			if (closeableRegistryClient.unregisterCloseable(currentStateHandleInStream)) {
				IOUtils.closeQuietly(currentStateHandleInStream);
			}
		}
	}

	/**
	 * Restore the KV-state / ColumnFamily meta data for all key-groups referenced by the current state handle.
	 */
	private void restoreKVStateMetaData() throws IOException, StateMigrationException {

		// isSerializerPresenceRequired flag is set to false, since for the RocksDB state backend,
		// deserialization of state happens lazily during runtime; we depend on the fact
		// that the new serializer for states could be compatible, and therefore the restore can continue
		// without old serializers required to be present.
		KeyedBackendSerializationProxy<K> serializationProxy =
			new KeyedBackendSerializationProxy<>(userCodeClassLoader, false);

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

		this.keygroupStreamCompressionDecorator = serializationProxy.isUsingKeyGroupCompression() ?
			SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;

		List<StateMetaInfoSnapshot> restoredMetaInfos = serializationProxy.getStateMetaInfoSnapshots();
		currentStateHandleKVStateColumnFamilies = new ArrayList<>(restoredMetaInfos.size());

		for (StateMetaInfoSnapshot restoredMetaInfo : restoredMetaInfos) {
			final String stateName = restoredMetaInfo.getName();

			StateColumnFamilyHandle stateColumnFamilyHandle = stateRocksDB.getStateColumnFamily(stateName);
			if (stateColumnFamilyHandle == null) {

				RegisteredStateMetaInfoBase stateMetaInfo =
					RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(restoredMetaInfo);

				rocksDBKeyedStateBackend.restoredKvStateMetaInfos.put(stateName, restoredMetaInfo);
				stateColumnFamilyHandle = stateRocksDB.createNewStateColumnFamily(stateMetaInfo, columnFamilyOptions);
			} else {
				// TODO with eager state registration in place, check here for serializer migration strategies
			}
			currentStateHandleKVStateColumnFamilies.add(stateColumnFamilyHandle.getColumnFamilyHandle());
		}
	}

	/**
	 * Restore the KV-state / ColumnFamily data for all key-groups referenced by the current state handle.
	 */
	private void restoreKVStateData() throws IOException, RocksDBException {
		//for all key-groups in the current state handle...
		try (RocksDBWriteBatchWrapper writeBatchWrapper = stateRocksDB.createSafeWriteBatch()) {
			for (Tuple2<Integer, Long> keyGroupOffset : currentKeyGroupsStateHandle.getGroupRangeOffsets()) {
				int keyGroup = keyGroupOffset.f0;

				// Check that restored key groups all belong to the backend
				Preconditions.checkState(backendKeyGroupRange.contains(keyGroup), "The key group must belong to the backend");

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
								writeBatchWrapper.put(handle, key, value);
								//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
								kvStateId = END_OF_KEY_GROUP_MARK
									& compressedKgInputView.readShort();
								if (END_OF_KEY_GROUP_MARK == kvStateId) {
									keyGroupHasMoreKeys = false;
								} else {
									handle = currentStateHandleKVStateColumnFamilies.get(kvStateId);
								}
							} else {
								writeBatchWrapper.put(handle, key, value);
							}
						}
					}
				}
			}
		}
	}
}
