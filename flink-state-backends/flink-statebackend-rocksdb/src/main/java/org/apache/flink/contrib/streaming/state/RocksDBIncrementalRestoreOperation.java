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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredKeyedBackendStateMetaInfo;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;

import static org.apache.flink.contrib.streaming.state.RocksDBSnapshotUtil.SST_FILE_SUFFIX;

/**
 * Encapsulates the process of restoring a RocksDBKeyedStateBackend from an incremental snapshot.
 */
public class RocksDBIncrementalRestoreOperation<T> extends RestoreOperationBase<T>{

//	private final RocksDBKeyedStateBackend<T> stateBackend;

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBIncrementalRestoreOperation.class);

	/** The column family options from the options factory. */
	private final ColumnFamilyOptions columnOptions;

	/** The DB options from the options factory. */
	private final DBOptions dbOptions;

	private final File instanceBasePath;

	private final File instanceRocksDBPath;

	private final int keyGroupPrefixBytes;

	private final SortedMap<Long, Set<StateHandleID>> materializedSstFiles;

	RocksDBIncrementalRestoreOperation(RocksDBKeyedStateBackend<T> stateBackend) {
//		this.stateBackend = stateBackend;
	}

	/**
	 * Root method that branches for different implementations of {@link KeyedStateHandle}.
	 */
	void restore(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {

		boolean hasExtraKeys = (restoreStateHandles.size() > 1 ||
			!Objects.equals(restoreStateHandles.iterator().next().getKeyGroupRange(), keyGroupRange));

//		if (hasExtraKeys) {
//			stateBackend.createDB();
//		}

		for (KeyedStateHandle rawStateHandle : restoreStateHandles) {

			if (rawStateHandle instanceof IncrementalKeyedStateHandle) {
				restoreInstance((IncrementalKeyedStateHandle) rawStateHandle, hasExtraKeys);
			} else if (rawStateHandle instanceof IncrementalLocalKeyedStateHandle) {
				Preconditions.checkState(!hasExtraKeys, "Cannot recover from local state after rescaling.");
				restoreInstance((IncrementalLocalKeyedStateHandle) rawStateHandle);
			} else {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected " + IncrementalKeyedStateHandle.class +
					", but found " + rawStateHandle.getClass());
			}
		}
	}

	/**
	 * Recovery from remote incremental state.
	 */
	private void restoreInstance(
		IncrementalKeyedStateHandle restoreStateHandle,
		boolean hasExtraKeys) throws Exception {

		// read state data
		Path temporaryRestoreInstancePath = new Path(
			instanceBasePath.getAbsolutePath(),
			UUID.randomUUID().toString());

		try {

			transferAllStateDataToDirectory(restoreStateHandle, temporaryRestoreInstancePath);

			// read meta data
			List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots =
				readMetaData(restoreStateHandle.getMetaStateHandle());

			List<ColumnFamilyDescriptor> columnFamilyDescriptors =
				createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots);

			if (hasExtraKeys) {
				restoreKeyGroupsShardWithTemporaryHelperInstance(
					temporaryRestoreInstancePath,
					columnFamilyDescriptors,
					stateMetaInfoSnapshots);
			} else {

				// since we transferred all remote state to a local directory, we can use the same code as for
				// local recovery.
				IncrementalLocalKeyedStateHandle localKeyedStateHandle = new IncrementalLocalKeyedStateHandle(
					restoreStateHandle.getBackendIdentifier(),
					restoreStateHandle.getCheckpointId(),
					new DirectoryStateHandle(temporaryRestoreInstancePath),
					restoreStateHandle.getKeyGroupRange(),
					restoreStateHandle.getMetaStateHandle(),
					restoreStateHandle.getSharedState().keySet());

				restoreLocalStateIntoFullInstance(
					localKeyedStateHandle,
					columnFamilyDescriptors,
					stateMetaInfoSnapshots);
			}
		} finally {
			FileSystem restoreFileSystem = temporaryRestoreInstancePath.getFileSystem();
			if (restoreFileSystem.exists(temporaryRestoreInstancePath)) {
				restoreFileSystem.delete(temporaryRestoreInstancePath, true);
			}
		}
	}

	/**
	 * Recovery from local incremental state.
	 */
	private void restoreInstance(IncrementalLocalKeyedStateHandle localKeyedStateHandle) throws Exception {
		// read meta data
		List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots =
			readMetaData(localKeyedStateHandle.getMetaDataState());

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots);

		restoreLocalStateIntoFullInstance(
			localKeyedStateHandle,
			columnFamilyDescriptors,
			stateMetaInfoSnapshots);
	}

	/**
	 * This method recreates and registers all {@link ColumnFamilyDescriptor} from Flink's state meta data snapshot.
	 */
	private List<ColumnFamilyDescriptor> createAndRegisterColumnFamilyDescriptors(
		List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots) {

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			new ArrayList<>(1 + stateMetaInfoSnapshots.size());

		for (RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> stateMetaInfoSnapshot : stateMetaInfoSnapshots) {

			ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
				stateMetaInfoSnapshot.getName().getBytes(ConfigConstants.DEFAULT_CHARSET),
				columnOptions);

			columnFamilyDescriptors.add(columnFamilyDescriptor);
			restoredKvStateMetaInfos.put(stateMetaInfoSnapshot.getName(), stateMetaInfoSnapshot);
		}
		return columnFamilyDescriptors;
	}

	/**
	 * This method implements the core of the restore logic that unifies how local and remote state are recovered.
	 */
	private void restoreLocalStateIntoFullInstance(
		IncrementalLocalKeyedStateHandle restoreStateHandle,
		List<ColumnFamilyDescriptor> columnFamilyDescriptors,
		List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots) throws Exception {
		// pick up again the old backend id, so the we can reference existing state
//		stateBacken.backendUID = restoreStateHandle.getBackendIdentifier();

		LOG.debug("Restoring keyed backend uid in operator {} from incremental snapshot to {}.",
			stateBackend.operatorIdentifier, stateBackend.backendUID);

		// create hard links in the instance directory
		if (!instanceRocksDBPath.mkdirs()) {
			throw new IOException("Could not create RocksDB data directory.");
		}

		Path restoreSourcePath = restoreStateHandle.getDirectoryStateHandle().getDirectory();
		restoreInstanceDirectoryFromPath(restoreSourcePath);

		List<ColumnFamilyHandle> columnFamilyHandles =
			new ArrayList<>(1 + columnFamilyDescriptors.size());

//		db = stateBackend.openDB(
//			instanceRocksDBPath.getAbsolutePath(),
//			columnFamilyDescriptors, columnFamilyHandles);
//
//		// extract and store the default column family which is located at the first index
//		stateBackend.defaultColumnFamily = columnFamilyHandles.remove(0);

		for (int i = 0; i < columnFamilyDescriptors.size(); ++i) {
			RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> stateMetaInfoSnapshot = stateMetaInfoSnapshots.get(i);

			ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);
			RegisteredKeyedBackendStateMetaInfo<?, ?> stateMetaInfo =
				new RegisteredKeyedBackendStateMetaInfo<>(
					stateMetaInfoSnapshot.getStateType(),
					stateMetaInfoSnapshot.getName(),
					stateMetaInfoSnapshot.getNamespaceSerializer(),
					stateMetaInfoSnapshot.getStateSerializer());

			kvStateInformation.put(
				stateMetaInfoSnapshot.getName(),
				new Tuple2<>(columnFamilyHandle, stateMetaInfo));
		}

		// use the restore sst files as the base for succeeding checkpoints
		synchronized (materializedSstFiles) {
			materializedSstFiles.put(
				restoreStateHandle.getCheckpointId(),
				restoreStateHandle.getSharedStateHandleIDs());
		}

//		stateBackend.lastCompletedCheckpointId = restoreStateHandle.getCheckpointId();
	}

	/**
	 * This recreates the new working directory of the recovered RocksDB instance and links/copies the contents from
	 * a local state.
	 */
	private void restoreInstanceDirectoryFromPath(Path source) throws IOException {

		FileSystem fileSystem = source.getFileSystem();

		final FileStatus[] fileStatuses = fileSystem.listStatus(source);

		if (fileStatuses == null) {
			throw new IOException("Cannot list file statues. Directory " + source + " does not exist.");
		}

		for (FileStatus fileStatus : fileStatuses) {
			final Path filePath = fileStatus.getPath();
			final String fileName = filePath.getName();
			File restoreFile = new File(source.getPath(), fileName);
			File targetFile = new File(instanceRocksDBPath.getPath(), fileName);
			if (fileName.endsWith(SST_FILE_SUFFIX)) {
				// hardlink'ing the immutable sst-files.
				Files.createLink(targetFile.toPath(), restoreFile.toPath());
			} else {
				// true copy for all other files.
				Files.copy(restoreFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			}
		}
	}

	/**
	 * Reads Flink's state meta data file from the state handle.
	 */
	private List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> readMetaData(
		StreamStateHandle metaStateHandle) throws Exception {

		FSDataInputStream inputStream = null;

		try {
			inputStream = metaStateHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(inputStream);

			KeyedBackendSerializationProxy<T> serializationProxy =
				new KeyedBackendSerializationProxy<>(userCodeClassLoader);
			DataInputView in = new DataInputViewStreamWrapper(inputStream);
			serializationProxy.read(in);

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

			return serializationProxy.getStateMetaInfoSnapshots();
		} finally {
			if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}
		}
	}

	private void transferAllStateDataToDirectory(
		IncrementalKeyedStateHandle restoreStateHandle,
		Path dest) throws IOException {

		final Map<StateHandleID, StreamStateHandle> sstFiles =
			restoreStateHandle.getSharedState();
		final Map<StateHandleID, StreamStateHandle> miscFiles =
			restoreStateHandle.getPrivateState();

		transferAllDataFromStateHandles(sstFiles, dest);
		transferAllDataFromStateHandles(miscFiles, dest);
	}

	/**
	 * Copies all the files from the given stream state handles to the given path, renaming the files w.r.t. their
	 * {@link StateHandleID}.
	 */
	private void transferAllDataFromStateHandles(
		Map<StateHandleID, StreamStateHandle> stateHandleMap,
		Path restoreInstancePath) throws IOException {

		for (Map.Entry<StateHandleID, StreamStateHandle> entry : stateHandleMap.entrySet()) {
			StateHandleID stateHandleID = entry.getKey();
			StreamStateHandle remoteFileHandle = entry.getValue();
			copyStateDataHandleData(new Path(restoreInstancePath, stateHandleID.toString()), remoteFileHandle);
		}
	}

	/**
	 * Copies the file from a single state handle to the given path.
	 */
	private void copyStateDataHandleData(
		Path restoreFilePath,
		StreamStateHandle remoteFileHandle) throws IOException {

		FileSystem restoreFileSystem = restoreFilePath.getFileSystem();

		FSDataInputStream inputStream = null;
		FSDataOutputStream outputStream = null;

		try {
			inputStream = remoteFileHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(inputStream);

			outputStream = restoreFileSystem.create(restoreFilePath, FileSystem.WriteMode.OVERWRITE);
			cancelStreamRegistry.registerCloseable(outputStream);

			byte[] buffer = new byte[8 * 1024];
			while (true) {
				int numBytes = inputStream.read(buffer);
				if (numBytes == -1) {
					break;
				}

				outputStream.write(buffer, 0, numBytes);
			}
		} finally {
			if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}

			if (cancelStreamRegistry.unregisterCloseable(outputStream)) {
				outputStream.close();
			}
		}
	}

	/**
	 * In case of rescaling, this method creates a temporary RocksDB instance for a key-groups shard. All contents
	 * from the temporary instance are copied into the real restore instance and then the temporary instance is
	 * discarded.
	 */
	private void restoreKeyGroupsShardWithTemporaryHelperInstance(
		Path restoreInstancePath,
		List<ColumnFamilyDescriptor> columnFamilyDescriptors,
		List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoSnapshots) throws Exception {

		List<ColumnFamilyHandle> columnFamilyHandles =
			new ArrayList<>(1 + columnFamilyDescriptors.size());

		//TODO maybe get rid of this static call and relocate that code elsewhere?
		try (RocksDB temporaryRestoreDb = RocksDBKeyedStateBackend.openDB(
			restoreInstancePath.getPath(),
			dbOptions,
			columnOptions,
			columnFamilyDescriptors,
			columnFamilyHandles)) {

			final ColumnFamilyHandle defaultColumnFamily = columnFamilyHandles.remove(0);

			Preconditions.checkState(columnFamilyHandles.size() == columnFamilyDescriptors.size());

			try {
				for (int i = 0; i < columnFamilyDescriptors.size(); ++i) {
					ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);
					ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptors.get(i);
					RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> stateMetaInfoSnapshot = stateMetaInfoSnapshots.get(i);

					Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> registeredStateMetaInfoEntry =
						kvStateInformation.get(stateMetaInfoSnapshot.getName());

					if (null == registeredStateMetaInfoEntry) {

						RegisteredKeyedBackendStateMetaInfo<?, ?> stateMetaInfo =
							new RegisteredKeyedBackendStateMetaInfo<>(
								stateMetaInfoSnapshot.getStateType(),
								stateMetaInfoSnapshot.getName(),
								stateMetaInfoSnapshot.getNamespaceSerializer(),
								stateMetaInfoSnapshot.getStateSerializer());

						registeredStateMetaInfoEntry =
							new Tuple2<>(
								db.createColumnFamily(columnFamilyDescriptor),
								stateMetaInfo);

						kvStateInformation.put(
							stateMetaInfoSnapshot.getName(),
							registeredStateMetaInfoEntry);
					}

					ColumnFamilyHandle targetColumnFamilyHandle = registeredStateMetaInfoEntry.f0;

					try (RocksIterator iterator = temporaryRestoreDb.newIterator(columnFamilyHandle)) {

						int startKeyGroup = keyGroupRange.getStartKeyGroup();
						byte[] startKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
						for (int j = 0; j < keyGroupPrefixBytes; ++j) {
							startKeyGroupPrefixBytes[j] = (byte) (startKeyGroup >>> ((keyGroupPrefixBytes - j - 1) * Byte.SIZE));
						}

						iterator.seek(startKeyGroupPrefixBytes);

						while (iterator.isValid()) {

							int keyGroup = 0;
							for (int j = 0; j < keyGroupPrefixBytes; ++j) {
								keyGroup = (keyGroup << Byte.SIZE) + iterator.key()[j];
							}

							if (keyGroupRange.contains(keyGroup)) {
								db.put(targetColumnFamilyHandle,
									writeOptions,
									iterator.key(), iterator.value());
							}

							iterator.next();
						}
					} // releases native iterator resources
				}
			} finally {

				//release native tmp db column family resources
				IOUtils.closeQuietly(defaultColumnFamily);

				for (ColumnFamilyHandle flinkColumnFamilyHandle : columnFamilyHandles) {
					IOUtils.closeQuietly(flinkColumnFamilyHandle);
				}
			}
		} // releases native tmp db resources
	}
}
