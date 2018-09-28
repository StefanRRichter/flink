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
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.StateColumnFamilyHandle;
import org.apache.flink.contrib.streaming.state.StateRocksDB;
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
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;

/**
 * Encapsulates the process of restoring a RocksDBKeyedStateBackend from an incremental snapshot.
 */
public class RocksDBIncrementalRestoreOperation<T> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBIncrementalRestoreOperation.class);

	private final RocksDBKeyedStateBackend<T> stateBackend;
	private final SortedMap<Long, Set<StateHandleID>> restoredSstFiles;

	private final DBOptions dbOptions;
	private final ColumnFamilyOptions columnFamilyOptions;
	private final KeyGroupRange keyGroupRange;
	private final int keyGroupPrefixBytes;
	private final File instanceBasePath;
	private final LinkedHashMap<String, StateColumnFamilyHandle> kvStateInformation;

	private UUID restoredBackendUID;
	private long lastCompletedCheckpointId;

	private RocksDBIncrementalRestoreOperation(RocksDBKeyedStateBackend<T> stateBackend) {

		this.stateBackend = stateBackend;
		this.restoredSstFiles = new TreeMap<>();
	}

	SortedMap<Long, Set<StateHandleID>> getRestoredSstFiles() {
		return restoredSstFiles;
	}

	UUID getRestoredBackendUID() {
		return restoredBackendUID;
	}

	long getLastCompletedCheckpointId() {
		return lastCompletedCheckpointId;
	}

	/**
	 * Root method that branches for different implementations of {@link KeyedStateHandle}.
	 */
	void restore(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {

		final KeyedStateHandle theFirstStateHandle = restoreStateHandles.iterator().next();

		boolean isRescaling = (restoreStateHandles.size() > 1 ||
			!Objects.equals(theFirstStateHandle.getKeyGroupRange(), keyGroupRange));

		if (!isRescaling) {
			restoreWithoutRescaling(theFirstStateHandle);
		} else {
			restoreWithRescaling(restoreStateHandles);
		}
	}

	/**
	 * Recovery from a single remote incremental state without rescaling.
	 */
	void restoreWithoutRescaling(KeyedStateHandle rawStateHandle) throws Exception {

		IncrementalLocalKeyedStateHandle localKeyedStateHandle;
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;
		List<ColumnFamilyDescriptor> columnFamilyDescriptors;

		// Recovery from remote incremental state.
		Path temporaryRestoreInstancePath = new Path(
			instanceBasePath.getAbsolutePath(),
			UUID.randomUUID().toString());

		try {
			if (rawStateHandle instanceof IncrementalKeyedStateHandle) {

				IncrementalKeyedStateHandle restoreStateHandle = (IncrementalKeyedStateHandle) rawStateHandle;

				// read state data.
				transferAllStateDataToDirectory(restoreStateHandle, temporaryRestoreInstancePath);

				stateMetaInfoSnapshots = readMetaData(restoreStateHandle.getMetaStateHandle());
				columnFamilyDescriptors = createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots);

				// since we transferred all remote state to a local directory, we can use the same code as for
				// local recovery.
				localKeyedStateHandle = new IncrementalLocalKeyedStateHandle(
					restoreStateHandle.getBackendIdentifier(),
					restoreStateHandle.getCheckpointId(),
					new DirectoryStateHandle(temporaryRestoreInstancePath),
					restoreStateHandle.getKeyGroupRange(),
					restoreStateHandle.getMetaStateHandle(),
					restoreStateHandle.getSharedState().keySet());
			} else if (rawStateHandle instanceof IncrementalLocalKeyedStateHandle) {

				// Recovery from local incremental state.
				localKeyedStateHandle = (IncrementalLocalKeyedStateHandle) rawStateHandle;
				stateMetaInfoSnapshots = readMetaData(localKeyedStateHandle.getMetaDataState());
				columnFamilyDescriptors = createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots);
			} else {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected " + IncrementalKeyedStateHandle.class + " or " + IncrementalLocalKeyedStateHandle.class +
					", but found " + rawStateHandle.getClass());
			}

			restoreLocalStateIntoFullInstance(
				localKeyedStateHandle,
				columnFamilyDescriptors,
				stateMetaInfoSnapshots);
		} finally {
			FileSystem restoreFileSystem = temporaryRestoreInstancePath.getFileSystem();
			if (restoreFileSystem.exists(temporaryRestoreInstancePath)) {
				restoreFileSystem.delete(temporaryRestoreInstancePath, true);
			}
		}
	}

	/**
	 * Recovery from multi incremental states with rescaling. For rescaling, this method creates a temporary
	 * RocksDB instance for a key-groups shard. All contents from the temporary instance are copied into the
	 * real restore instance and then the temporary instance is discarded.
	 */
	void restoreWithRescaling(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {

		this.restoredBackendUID = UUID.randomUUID();

		initTargetDB(restoreStateHandles, keyGroupRange);

		byte[] startKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(stateBackend.getKeyGroupRange().getStartKeyGroup(), startKeyGroupPrefixBytes);

		byte[] stopKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getEndKeyGroup() + 1, stopKeyGroupPrefixBytes);

		for (KeyedStateHandle rawStateHandle : restoreStateHandles) {

			if (!(rawStateHandle instanceof IncrementalKeyedStateHandle)) {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected " + IncrementalKeyedStateHandle.class +
					", but found " + rawStateHandle.getClass());
			}

			Path temporaryRestoreInstancePath = new Path(instanceBasePath.getAbsolutePath() + UUID.randomUUID().toString());
			try (StateRocksDB tmpStateRocksDB = restoreDBInstanceFromStateHandle(
				(IncrementalKeyedStateHandle) rawStateHandle,
				temporaryRestoreInstancePath);
				 RocksDBWriteBatchWrapper writeBatchWrapper = tmpStateRocksDB.createSafeWriteBatch()) {

//				List<ColumnFamilyDescriptor> tmpColumnFamilyDescriptors = tmpStateRocksDB.columnFamilyDescriptors;
//				List<ColumnFamilyHandle> tmpColumnFamilyHandles = tmpStateRocksDB.columnFamilyHandles;

				// iterating only the requested descriptors automatically skips the default column family handle
				for (int i = 0; i < tmpColumnFamilyDescriptors.size(); ++i) {
					ColumnFamilyHandle tmpColumnFamilyHandle = tmpColumnFamilyHandles.get(i);
					ColumnFamilyDescriptor tmpColumnFamilyDescriptor = tmpColumnFamilyDescriptors.get(i);

					ColumnFamilyHandle targetColumnFamilyHandle = getOrRegisterColumnFamilyHandle(
						tmpColumnFamilyDescriptor, null, tmpStateRocksDB.stateMetaInfoSnapshots.get(i));

					try (RocksIteratorWrapper iterator = getRocksIterator(tmpStateRocksDB.db, tmpColumnFamilyHandle)) {

						iterator.seek(startKeyGroupPrefixBytes);

						while (iterator.isValid()) {

							if (RocksDBIncrementalCheckpointUtils.beforeThePrefixBytes(iterator.key(), stopKeyGroupPrefixBytes)) {
								writeBatchWrapper.put(targetColumnFamilyHandle, iterator.key(), iterator.value());
							} else {
								// Since the iterator will visit the record according to the sorted order,
								// we can just break here.
								break;
							}

							iterator.next();
						}
					} // releases native iterator resources
				}
			} finally {
				FileSystem restoreFileSystem = temporaryRestoreInstancePath.getFileSystem();
				if (restoreFileSystem.exists(temporaryRestoreInstancePath)) {
					restoreFileSystem.delete(temporaryRestoreInstancePath, true);
				}
			}
		}
	}

	private class RestoredDBInstance implements AutoCloseable {

		@Nonnull
		private final RocksDB db;

		@Nonnull
		private final List<ColumnFamilyHandle> columnFamilyHandles;

		@Nonnull
		private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;

		@Nonnull
		private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

		private RestoredDBInstance(
			@Nonnull RocksDB db,
			@Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
			@Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
			@Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
			this.db = db;
			columnFamilyHandles.remove(0);
			this.columnFamilyHandles = columnFamilyHandles;
			this.columnFamilyDescriptors = columnFamilyDescriptors;
			this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
		}

		@Override
		public void close() {

			IOUtils.closeQuietly(db.getDefaultColumnFamily());

			for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
				IOUtils.closeQuietly(columnFamilyHandle);
			}

			IOUtils.closeQuietly(db);
		}
	}

	private StateRocksDB restoreDBInstanceFromStateHandle(
		IncrementalKeyedStateHandle restoreStateHandle,
		Path temporaryRestoreInstancePath) throws Exception {

		transferAllStateDataToDirectory(restoreStateHandle, temporaryRestoreInstancePath);

		// read meta data
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
			readMetaData(restoreStateHandle.getMetaStateHandle());

//		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
//			createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots);
//
//		List<ColumnFamilyHandle> columnFamilyHandles =
//			new ArrayList<>(stateMetaInfoSnapshots.size() + 1);

		List<RegisteredStateMetaInfoBase> stateMetaInfoBases =
			stateMetaInfoSnapshots.stream()
				.map(RegisteredStateMetaInfoBase::fromMetaInfoSnapshot)
				.collect(Collectors.toList());

		return StateRocksDB.createStateRocksDB(
			dbOptions,
			columnFamilyOptions,
			new File(temporaryRestoreInstancePath.getPath()),
			stateMetaInfoBases);

//		RocksDB restoreDb = stateBackend.openDB(
//			temporaryRestoreInstancePath.getPath(),
//			columnFamilyDescriptors,
//			columnFamilyHandles);
//		return new RestoredDBInstance(restoreDb, columnFamilyHandles, columnFamilyDescriptors, stateMetaInfoSnapshots);
	}

	private ColumnFamilyHandle getOrRegisterColumnFamilyHandle(
		ColumnFamilyDescriptor columnFamilyDescriptor,
		ColumnFamilyHandle columnFamilyHandle,
		StateMetaInfoSnapshot stateMetaInfoSnapshot) throws RocksDBException {

		StateColumnFamilyHandle stateColumnFamilyHandle =
			kvStateInformation.get(stateMetaInfoSnapshot.getName());

		if (null == stateColumnFamilyHandle) {
			RegisteredStateMetaInfoBase stateMetaInfo =
				RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);

			stateColumnFamilyHandle =
				new StateColumnFamilyHandle(
					columnFamilyHandle != null ? columnFamilyHandle : stateBackend.db.createColumnFamily(columnFamilyDescriptor),
					stateMetaInfo);

			kvStateInformation.put(
				stateMetaInfoSnapshot.getName(),
				stateColumnFamilyHandle);
		}

		return stateColumnFamilyHandle.getColumnFamilyHandle();
	}

	/**
	 * This method first try to find a initial handle to init the target db, if the initial handle
	 * is not null, we just init the target db with the handle and clip it with the target key-group
	 * range. If the initial handle is null we create a empty db as the target db.
	 */
	private void initTargetDB(
		Collection<KeyedStateHandle> restoreStateHandles,
		KeyGroupRange targetKeyGroupRange) throws Exception {

		IncrementalKeyedStateHandle initialHandle = (IncrementalKeyedStateHandle) RocksDBIncrementalCheckpointUtils.chooseTheBestStateHandleForInitial(
			restoreStateHandles, targetKeyGroupRange);

		Preconditions.checkNotNull(initialHandle);

		restoreStateHandles.remove(initialHandle);
		RestoredDBInstance restoreDBInfo = null;
		Path instancePath = new Path(instanceRocksDBPath.getAbsolutePath());
		try {
			restoreDBInfo = restoreDBInstanceFromStateHandle(
				initialHandle,
				instancePath);

			RocksDBIncrementalCheckpointUtils.clipDBWithKeyGroupRange(
				restoreDBInfo.db,
				restoreDBInfo.columnFamilyHandles,
				targetKeyGroupRange,
				initialHandle.getKeyGroupRange(),
				keyGroupPrefixBytes);

			stateBackend.db = restoreDBInfo.db;
			stateBackend.writeBatchWrapper =
				new RocksDBWriteBatchWrapper(stateBackend.db, stateBackend.writeOptions);

			for (int i = 0; i < restoreDBInfo.stateMetaInfoSnapshots.size(); ++i) {
				getOrRegisterColumnFamilyHandle(
					restoreDBInfo.columnFamilyDescriptors.get(i),
					restoreDBInfo.columnFamilyHandles.get(i),
					restoreDBInfo.stateMetaInfoSnapshots.get(i));
			}
		} catch (Exception e) {
			if (restoreDBInfo != null) {
				restoreDBInfo.close();
			}
			FileSystem restoreFileSystem = instancePath.getFileSystem();
			if (restoreFileSystem.exists(instancePath)) {
				restoreFileSystem.delete(instancePath, true);
			}
			throw e;
		}
	}

	/**
	 * This method recreates and registers all {@link ColumnFamilyDescriptor} from Flink's state meta data snapshot.
	 */
	private List<ColumnFamilyDescriptor> createAndRegisterColumnFamilyDescriptors(
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			new ArrayList<>(stateMetaInfoSnapshots.size());

		for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {

			ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
				stateMetaInfoSnapshot.getName().getBytes(ConfigConstants.DEFAULT_CHARSET),
				stateBackend.columnOptions);

			columnFamilyDescriptors.add(columnFamilyDescriptor);
			stateBackend.restoredKvStateMetaInfos.put(stateMetaInfoSnapshot.getName(), stateMetaInfoSnapshot);
		}
		return columnFamilyDescriptors;
	}

	/**
	 * This method implements the core of the restore logic that unifies how local and remote state are recovered.
	 */
	private void restoreLocalStateIntoFullInstance(
		IncrementalLocalKeyedStateHandle restoreStateHandle,
		List<ColumnFamilyDescriptor> columnFamilyDescriptors,
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) throws Exception {
		// pick up again the old backend id, so the we can reference existing state
		this.restoredBackendUID = restoreStateHandle.getBackendIdentifier();

		LOG.debug("Restoring keyed backend uid in operator {} from incremental snapshot to {}.",
			stateBackend.operatorIdentifier, this.restoredBackendUID);

		// create hard links in the instance directory
		if (!stateBackend.instanceRocksDBPath.mkdirs()) {
			throw new IOException("Could not create RocksDB data directory.");
		}

		Path restoreSourcePath = restoreStateHandle.getDirectoryStateHandle().getDirectory();
		restoreInstanceDirectoryFromPath(restoreSourcePath);

		List<ColumnFamilyHandle> columnFamilyHandles =
			new ArrayList<>(1 + columnFamilyDescriptors.size());

		stateBackend.db = stateBackend.openDB(
			stateBackend.instanceRocksDBPath.getAbsolutePath(),
			columnFamilyDescriptors, columnFamilyHandles);

		// remove the default column family which is located at the first index
		columnFamilyHandles.remove(0);
		stateBackend.writeBatchWrapper = new RocksDBWriteBatchWrapper(stateBackend.db, stateBackend.writeOptions);

		for (int i = 0; i < columnFamilyDescriptors.size(); ++i) {
			StateMetaInfoSnapshot stateMetaInfoSnapshot = stateMetaInfoSnapshots.get(i);

			ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);
			RegisteredStateMetaInfoBase stateMetaInfo =
				RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);

			stateBackend.kvStateInformation.put(
				stateMetaInfoSnapshot.getName(),
				new StateColumnFamilyHandle(columnFamilyHandle, stateMetaInfo));
		}

		// use the restore sst files as the base for succeeding checkpoints
		restoredSstFiles.put(
			restoreStateHandle.getCheckpointId(),
			restoreStateHandle.getSharedStateHandleIDs());

		lastCompletedCheckpointId = restoreStateHandle.getCheckpointId();
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
			File targetFile = new File(stateBackend.instanceRocksDBPath.getPath(), fileName);
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
	private List<StateMetaInfoSnapshot> readMetaData(
		StreamStateHandle metaStateHandle) throws Exception {

		FSDataInputStream inputStream = null;

		try {
			inputStream = metaStateHandle.openInputStream();
			stateBackend.cancelStreamRegistry.registerCloseable(inputStream);

			// isSerializerPresenceRequired flag is set to false, since for the RocksDB state backend,
			// deserialization of state happens lazily during runtime; we depend on the fact
			// that the new serializer for states could be compatible, and therefore the restore can continue
			// without old serializers required to be present.
			KeyedBackendSerializationProxy<T> serializationProxy =
				new KeyedBackendSerializationProxy<>(stateBackend.userCodeClassLoader, false);
			DataInputView in = new DataInputViewStreamWrapper(inputStream);
			serializationProxy.read(in);

			// check for key serializer compatibility; this also reconfigures the
			// key serializer to be compatible, if it is required and is possible
			if (CompatibilityUtil.resolveCompatibilityResult(
				serializationProxy.getKeySerializer(),
				UnloadableDummyTypeSerializer.class,
				serializationProxy.getKeySerializerConfigSnapshot(),
				stateBackend.keySerializer)
				.isRequiresMigration()) {

				// TODO replace with state migration; note that key hash codes need to remain the same after migration
				throw new StateMigrationException("The new key serializer is not compatible to read previous keys. " +
					"Aborting now since state migration is currently not available");
			}

			return serializationProxy.getStateMetaInfoSnapshots();
		} finally {
			if (stateBackend.cancelStreamRegistry.unregisterCloseable(inputStream)) {
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
			stateBackend.cancelStreamRegistry.registerCloseable(inputStream);

			outputStream = restoreFileSystem.create(restoreFilePath, FileSystem.WriteMode.OVERWRITE);
			stateBackend.cancelStreamRegistry.registerCloseable(outputStream);

			byte[] buffer = new byte[8 * 1024];
			while (true) {
				int numBytes = inputStream.read(buffer);
				if (numBytes == -1) {
					break;
				}

				outputStream.write(buffer, 0, numBytes);
			}
		} finally {
			if (stateBackend.cancelStreamRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}

			if (stateBackend.cancelStreamRegistry.unregisterCloseable(outputStream)) {
				outputStream.close();
			}
		}
	}
}
