/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyedBackendStateMetaInfo;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An {@link AbstractKeyedStateBackend} that stores its state in {@code RocksDB} and serializes state to
 * streams provided by a {@link org.apache.flink.runtime.state.CheckpointStreamFactory} upon
 * checkpointing. This state backend can store very large state that exceeds memory and spills
 * to disk. Except for the snapshotting, this class should be accessed as if it is not threadsafe.
 *
 * <p>This class follows the rules for closing/releasing native RocksDB resources as described in
 + <a href="https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families">
 * this document</a>.
 */
public class RocksDBKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBKeyedStateBackend.class);

	/** The name of the merge operator in RocksDB. Do not change except you know exactly what you do. */
	public static final String MERGE_OPERATOR_NAME = "stringappendtest";

//	/** String that identifies the operator that owns this backend. */
//	private final String operatorIdentifier;

	/** The column family options from the options factory. */
	private final ColumnFamilyOptions columnOptions;

	/** The DB options from the options factory. */
	private final DBOptions dbOptions;

	/** Path where this configured instance stores its data directory. */
	private final File instanceBasePath;

//	/** Path where this configured instance stores its RocksDB database. */
//	private final File instanceRocksDBPath;

	/**
	 * Protects access to RocksDB in other threads, like the checkpointing thread from parallel call that disposes the
	 * RocksDb object.
	 */
	private final ResourceGuard rocksDBResourceGuard;

	/**
	 * Our RocksDB database, this is used by the actual subclasses of {@link AbstractRocksDBState}
	 * to store state. The different k/v states that we have don't each have their own RocksDB
	 * instance. They all write to this instance but to their own column family.
	 */
	protected RocksDB db;

	/**
	 * We are not using the default column family for Flink state ops, but we still need to remember this handle so that
	 * we can close it properly when the backend is closed. This is required by RocksDB's native memory management.
	 */
	private ColumnFamilyHandle defaultColumnFamily;

	/**
	 * The write options to use in the states. We disable write ahead logging.
	 */
	private final WriteOptions writeOptions;

	/**
	 * Information about the k/v states as we create them. This is used to retrieve the
	 * column family that is used for a state and also for sanity checks when restoring.
	 */
	private final LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation;

	/**
	 * Map of state names to their corresponding restored state meta info.
	 *
	 * <p>TODO this map can be removed when eager-state registration is in place.
	 * TODO we currently need this cached to check state migration strategies when new serializers are registered.
	 */
	private final Map<String, RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredKvStateMetaInfos;

	/** Number of bytes required to prefix the key groups. */
	private final int keyGroupPrefixBytes;

	/** True if incremental checkpointing is enabled. */
	private final boolean enableIncrementalCheckpointing;

	/** The state handle ids of all sst files materialized in snapshots for previous checkpoints. */
	private final SortedMap<Long, Set<StateHandleID>> materializedSstFiles;

	/** The identifier of the last completed checkpoint. */
	private long lastCompletedCheckpointId = -1L;

//	/** Unique ID of this backend. */
//	private UUID backendUID;

	/** The snapshot strategy, e.g., if we use full or incremental checkpoints, local state, and so on. */
	private final SnapshotStrategy<SnapshotResult<KeyedStateHandle>> snapshotStrategy;

	public static class Builder<K> {

		String operatorIdentifier;
		ClassLoader userCodeClassLoader;
		File instanceBasePath;
		DBOptions dbOptions;
		ColumnFamilyOptions columnFamilyOptions;
		TaskKvStateRegistry kvStateRegistry;
		TypeSerializer<K> keySerializer;
		int numberOfKeyGroups;
		KeyGroupRange keyGroupRange;
		ExecutionConfig executionConfig;
		boolean enableIncrementalCheckpointing;
		LocalRecoveryConfig localRecoveryConfig;

		public Builder(
			String operatorIdentifier,
			ClassLoader userCodeClassLoader,
			File instanceBasePath,
			DBOptions dbOptions,
			ColumnFamilyOptions columnFamilyOptions,
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			ExecutionConfig executionConfig,
			boolean enableIncrementalCheckpointing,
			LocalRecoveryConfig localRecoveryConfig) {

			this.operatorIdentifier = operatorIdentifier;
			this.userCodeClassLoader = userCodeClassLoader;
			this.instanceBasePath = instanceBasePath;
			this.dbOptions = dbOptions;
			this.columnFamilyOptions = columnFamilyOptions;
			this.kvStateRegistry = kvStateRegistry;
			this.keySerializer = keySerializer;
			this.numberOfKeyGroups = numberOfKeyGroups;
			this.keyGroupRange = keyGroupRange;
			this.executionConfig = executionConfig;
			this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
			this.localRecoveryConfig = localRecoveryConfig;
		}

		public RocksDBKeyedStateBackend build() throws IOException {
			File instanceRocksDBPath = new File(instanceBasePath, "db");

			checkAndCreateDirectory(instanceBasePath);

			if (instanceRocksDBPath.exists()) {
				// Clear the base directory when the backend is created
				// in case something crashed and the backend never reached dispose()
				LOG.info("Deleting existing instance base directory {}.", instanceBasePath);

				try {
					FileUtils.deleteDirectory(instanceBasePath);
				} catch (IOException ex) {
					LOG.warn("Could not delete instance base path for RocksDB: " + instanceBasePath, ex);
				}
			}

			List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);

			RocksDB db = openDB(
				instanceRocksDBPath.getAbsolutePath(),
				dbOptions,
				columnFamilyOptions,
				Collections.emptyList(),
				columnFamilyHandles);

			ColumnFamilyHandle defaultColumnFamilyHandle = columnFamilyHandles.get(0);

			WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);

			final LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation = new LinkedHashMap<>();
			final Map<String, RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredKvStateMetaInfos = new HashMap<>();

			final SortedMap<Long, Set<StateHandleID>> materializedSstFiles = new TreeMap<>();

			SnapshotStrategy<SnapshotResult<KeyedStateHandle>> snapshotStrategy = enableIncrementalCheckpointing ?
				new IncrementalSnapshotStrategy() :
				new FullSnapshotStrategy();

			long lastCompletedCheckpointId = -1;

			return new RocksDBKeyedStateBackend<>(
				UUID.randomUUID(),
				kvStateRegistry,
				keySerializer,
				userCodeClassLoader,
				numberOfKeyGroups,
				keyGroupRange,
				executionConfig,
				operatorIdentifier,
				columnFamilyOptions,
				dbOptions,
				instanceBasePath,
				instanceRocksDBPath,
				new ResourceGuard(),
				db,
				defaultColumnFamilyHandle,
				writeOptions,
				kvStateInformation,
				restoredKvStateMetaInfos,
				enableIncrementalCheckpointing,
				materializedSstFiles,
				lastCompletedCheckpointId,
				localRecoveryConfig,
				snapshotStrategy);
		}
	}

	public RocksDBKeyedStateBackend(
		String operatorIdentifier,
		ClassLoader userCodeClassLoader,
		File instanceBasePath,
		DBOptions dbOptions,
		ColumnFamilyOptions columnFamilyOptions,
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		ExecutionConfig executionConfig,
		boolean enableIncrementalCheckpointing,
		LocalRecoveryConfig localRecoveryConfig
	) throws IOException {

		super(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups, keyGroupRange, executionConfig);

		this.operatorIdentifier = Preconditions.checkNotNull(operatorIdentifier);

		this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
		this.rocksDBResourceGuard = new ResourceGuard();

		// ensure that we use the right merge operator, because other code relies on this
		this.columnOptions = Preconditions.checkNotNull(columnFamilyOptions)
			.setMergeOperatorName(MERGE_OPERATOR_NAME);

		this.dbOptions = Preconditions.checkNotNull(dbOptions);

		this.instanceBasePath = Preconditions.checkNotNull(instanceBasePath);
		this.instanceRocksDBPath = new File(instanceBasePath, "db");

		checkAndCreateDirectory(instanceBasePath);

		if (instanceRocksDBPath.exists()) {
			// Clear the base directory when the backend is created
			// in case something crashed and the backend never reached dispose()
			cleanInstanceBasePath();
		}

		this.localRecoveryConfig = Preconditions.checkNotNull(localRecoveryConfig);
		this.keyGroupPrefixBytes = getNumberOfKeyGroups() > (Byte.MAX_VALUE + 1) ? 2 : 1;
		this.kvStateInformation = new LinkedHashMap<>();
		this.restoredKvStateMetaInfos = new HashMap<>();
		this.materializedSstFiles = new TreeMap<>();
		this.backendUID = UUID.randomUUID();

		this.snapshotStrategy = enableIncrementalCheckpointing ?
			new IncrementalSnapshotStrategy() :
			new FullSnapshotStrategy();

		this.writeOptions = new WriteOptions().setDisableWAL(true);

		LOG.debug("Setting initial keyed backend uid for operator {} to {}.", this.operatorIdentifier, this.backendUID);
	}

	public RocksDBKeyedStateBackend(
		UUID backendUID,
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		ClassLoader userCodeClassLoader,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		ExecutionConfig executionConfig,
		String operatorIdentifier,
		ColumnFamilyOptions columnOptions,
		DBOptions dbOptions,
		File instanceBasePath,
		File instanceRocksDBPath,
		ResourceGuard rocksDBResourceGuard,
		RocksDB db,
		ColumnFamilyHandle defaultColumnFamily,
		WriteOptions writeOptions,
		LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation,
		Map<String, RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredKvStateMetaInfos,
		boolean enableIncrementalCheckpointing,
		SortedMap<Long, Set<StateHandleID>> materializedSstFiles,
		long lastCompletedCheckpointId,
		LocalRecoveryConfig localRecoveryConfig,
		SnapshotStrategy<SnapshotResult<KeyedStateHandle>> snapshotStrategy) {

		super(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups, keyGroupRange, executionConfig);

		this.backendUID = backendUID;
		this.operatorIdentifier = operatorIdentifier;
		this.columnOptions = columnOptions;
		this.dbOptions = dbOptions;
		this.instanceBasePath = instanceBasePath;
		this.instanceRocksDBPath = instanceRocksDBPath;
		this.rocksDBResourceGuard = rocksDBResourceGuard;
		this.db = db;
		this.defaultColumnFamily = defaultColumnFamily;
		this.writeOptions = writeOptions;
		this.kvStateInformation = kvStateInformation;
		this.restoredKvStateMetaInfos = restoredKvStateMetaInfos;
		this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
		this.materializedSstFiles = materializedSstFiles;
		this.lastCompletedCheckpointId = lastCompletedCheckpointId;
		this.localRecoveryConfig = localRecoveryConfig;
		this.snapshotStrategy = snapshotStrategy;
		this.keyGroupPrefixBytes = numberOfKeyGroups > (Byte.MAX_VALUE + 1) ? 2 : 1;;
	}

	private static void checkAndCreateDirectory(File directory) throws IOException {
		if (directory.exists()) {
			if (!directory.isDirectory()) {
				throw new IOException("Not a directory: " + directory);
			}
		} else {
			if (!directory.mkdirs()) {
				throw new IOException(
					String.format("Could not create RocksDB data directory at %s.", directory));
			}
		}
	}

	@Override
	public <N> Stream<K> getKeys(String state, N namespace) {
		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> columnInfo = kvStateInformation.get(state);
		if (columnInfo == null) {
			return Stream.empty();
		}

		final TypeSerializer<N> namespaceSerializer = (TypeSerializer<N>) columnInfo.f1.getNamespaceSerializer();
		final ByteArrayOutputStreamWithPos namespaceOutputStream = new ByteArrayOutputStreamWithPos(8);
		boolean ambiguousKeyPossible = RocksDBKeySerializationUtils.isAmbiguousKeyPossible(keySerializer, namespaceSerializer);
		final byte[] nameSpaceBytes;
		try {
			RocksDBKeySerializationUtils.writeNameSpace(
				namespace,
				namespaceSerializer,
				namespaceOutputStream,
				new DataOutputViewStreamWrapper(namespaceOutputStream),
				ambiguousKeyPossible);
			nameSpaceBytes = namespaceOutputStream.toByteArray();
		} catch (IOException ex) {
			throw new FlinkRuntimeException("Failed to get keys from RocksDB state backend.", ex);
		}

		RocksIterator iterator = db.newIterator(columnInfo.f0);
		iterator.seekToFirst();

		final RocksIteratorForKeysWrapper<K> iteratorWrapper = new RocksIteratorForKeysWrapper<>(iterator, state, keySerializer, keyGroupPrefixBytes,
			ambiguousKeyPossible, nameSpaceBytes);

		Stream<K> targetStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iteratorWrapper, Spliterator.ORDERED), false);
		return targetStream.onClose(iteratorWrapper::close);
	}

	@VisibleForTesting
	ColumnFamilyHandle getColumnFamilyHandle(String state) {
		Tuple2<ColumnFamilyHandle, ?> columnInfo = kvStateInformation.get(state);
		return columnInfo != null ? columnInfo.f0 : null;
	}

	/**
	 * Should only be called by one thread, and only after all accesses to the DB happened.
	 */
	@Override
	public void dispose() {
		super.dispose();

		// This call will block until all clients that still acquire access to the RocksDB instance have released it,
		// so that we cannot release the native resources while clients are still working with it in parallel.
		rocksDBResourceGuard.close();

		// IMPORTANT: null reference to signal potential async checkpoint workers that the db was disposed, as
		// working on the disposed object results in SEGFAULTS.
		if (db != null) {

			// RocksDB's native memory management requires that *all* CFs (including default) are closed before the
			// DB is closed. See:
			// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
			// Start with default CF ...
			IOUtils.closeQuietly(defaultColumnFamily);

			// ... continue with the ones created by Flink...
			for (Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> columnMetaData :
				kvStateInformation.values()) {
				IOUtils.closeQuietly(columnMetaData.f0);
			}

			// ... and finally close the DB instance ...
			IOUtils.closeQuietly(db);

			// invalidate the reference
			db = null;

			IOUtils.closeQuietly(columnOptions);
			IOUtils.closeQuietly(dbOptions);
			IOUtils.closeQuietly(writeOptions);
			kvStateInformation.clear();
			restoredKvStateMetaInfos.clear();

			cleanInstanceBasePath();
		}
	}

	private void cleanInstanceBasePath() {
		LOG.info("Deleting existing instance base directory {}.", instanceBasePath);

		try {
			FileUtils.deleteDirectory(instanceBasePath);
		} catch (IOException ex) {
			LOG.warn("Could not delete instance base path for RocksDB: " + instanceBasePath, ex);
		}
	}

	public int getKeyGroupPrefixBytes() {
		return keyGroupPrefixBytes;
	}

	public WriteOptions getWriteOptions() {
		return writeOptions;
	}

	/**
	 * Triggers an asynchronous snapshot of the keyed state backend from RocksDB. This snapshot can be canceled and
	 * is also stopped when the backend is closed through {@link #dispose()}. For each backend, this method must always
	 * be called by the same thread.
	 *
	 * @param checkpointId  The Id of the checkpoint.
	 * @param timestamp     The timestamp of the checkpoint.
	 * @param streamFactory The factory that we can use for writing our state to streams.
	 * @param checkpointOptions Options for how to perform this checkpoint.
	 * @return Future to the state handle of the snapshot data.
	 * @throws Exception indicating a problem in the synchronous part of the checkpoint.
	 */
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		final long checkpointId,
		final long timestamp,
		final CheckpointStreamFactory streamFactory,
		CheckpointOptions checkpointOptions) throws Exception {

		return snapshotStrategy.performSnapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
	}

	@Override
	public void restore(Collection<KeyedStateHandle> restoreState) throws Exception {
		LOG.info("Initializing RocksDB keyed state backend.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Restoring snapshot from state handles: {}.", restoreState);
		}

		// clear all meta data
		kvStateInformation.clear();
		restoredKvStateMetaInfos.clear();

		try {
			if (restoreState == null || restoreState.isEmpty()) {
				createDB();
			} else {
				KeyedStateHandle firstStateHandle = restoreState.iterator().next();
				if (firstStateHandle instanceof IncrementalKeyedStateHandle
					|| firstStateHandle instanceof IncrementalLocalKeyedStateHandle) {
					RocksDBIncrementalRestoreOperation<K> restoreOperation = new RocksDBIncrementalRestoreOperation<>(this);
					restoreOperation.restore(restoreState);
				} else {
					RocksDBFullRestoreOperation<K> restoreOperation = new RocksDBFullRestoreOperation<>(this);
					restoreOperation.doRestore(restoreState);
				}
			}
		} catch (Exception ex) {
			dispose();
			throw ex;
		}
	}

	@Override
	public void notifyCheckpointComplete(long completedCheckpointId) {

		if (!enableIncrementalCheckpointing) {
			return;
		}

		synchronized (materializedSstFiles) {

			if (completedCheckpointId < lastCompletedCheckpointId) {
				return;
			}

			materializedSstFiles.keySet().removeIf(checkpointId -> checkpointId < completedCheckpointId);

			lastCompletedCheckpointId = completedCheckpointId;
		}
	}

	private void createDB() throws IOException {
		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
		this.db = openDB(instanceRocksDBPath.getAbsolutePath(), dbOptions, columnOptions, Collections.emptyList(), columnFamilyHandles);
		this.defaultColumnFamily = columnFamilyHandles.get(0);
	}

	static RocksDB openDB(
		String path,
		DBOptions dbOptions,
		ColumnFamilyOptions columnOptions,
		List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
		List<ColumnFamilyHandle> stateColumnFamilyHandles) throws IOException {

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			new ArrayList<>(1 + stateColumnFamilyDescriptors.size());

		// we add the required descriptor for the default CF in FIRST position, see
		// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnOptions));
		columnFamilyDescriptors.addAll(stateColumnFamilyDescriptors);

		RocksDB dbRef;

		try {
			dbRef = RocksDB.open(
				Preconditions.checkNotNull(dbOptions),
				Preconditions.checkNotNull(path),
				columnFamilyDescriptors,
				stateColumnFamilyHandles);
		} catch (RocksDBException e) {
			throw new IOException("Error while opening RocksDB instance.", e);
		}

		// requested + default CF
		Preconditions.checkState(1 + stateColumnFamilyDescriptors.size() == stateColumnFamilyHandles.size(),
			"Not all requested column family handles have been created");

		return dbRef;
	}

	// ------------------------------------------------------------------------
	//  State factories
	// ------------------------------------------------------------------------

	/**
	 * Creates a column family handle for use with a k/v state. When restoring from a snapshot
	 * we don't restore the individual k/v states, just the global RocksDB database and the
	 * list of column families. When a k/v state is first requested we check here whether we
	 * already have a column family for that and return it or create a new one if it doesn't exist.
	 *
	 * <p>This also checks whether the {@link StateDescriptor} for a state matches the one
	 * that we checkpointed, i.e. is already in the map of column families.
	 */
	@SuppressWarnings("rawtypes, unchecked")
	protected <N, S> ColumnFamilyHandle getColumnFamily(
		StateDescriptor<?, S> descriptor, TypeSerializer<N> namespaceSerializer) throws IOException, StateMigrationException {

		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> stateInfo =
			kvStateInformation.get(descriptor.getName());

		RegisteredKeyedBackendStateMetaInfo<N, S> newMetaInfo = new RegisteredKeyedBackendStateMetaInfo<>(
			descriptor.getType(),
			descriptor.getName(),
			namespaceSerializer,
			descriptor.getSerializer());

		if (stateInfo != null) {
			// TODO with eager registration in place, these checks should be moved to restore()

			RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> restoredMetaInfo =
				(RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S>) restoredKvStateMetaInfos.get(descriptor.getName());

			Preconditions.checkState(
				Objects.equals(newMetaInfo.getName(), restoredMetaInfo.getName()),
				"Incompatible state names. " +
					"Was [" + restoredMetaInfo.getName() + "], " +
					"registered with [" + newMetaInfo.getName() + "].");

			if (!Objects.equals(newMetaInfo.getStateType(), StateDescriptor.Type.UNKNOWN)
				&& !Objects.equals(restoredMetaInfo.getStateType(), StateDescriptor.Type.UNKNOWN)) {

				Preconditions.checkState(
					newMetaInfo.getStateType() == restoredMetaInfo.getStateType(),
					"Incompatible state types. " +
						"Was [" + restoredMetaInfo.getStateType() + "], " +
						"registered with [" + newMetaInfo.getStateType() + "].");
			}

			// check compatibility results to determine if state migration is required
			CompatibilityResult<N> namespaceCompatibility = CompatibilityUtil.resolveCompatibilityResult(
				restoredMetaInfo.getNamespaceSerializer(),
				null,
				restoredMetaInfo.getNamespaceSerializerConfigSnapshot(),
				newMetaInfo.getNamespaceSerializer());

			CompatibilityResult<S> stateCompatibility = CompatibilityUtil.resolveCompatibilityResult(
				restoredMetaInfo.getStateSerializer(),
				UnloadableDummyTypeSerializer.class,
				restoredMetaInfo.getStateSerializerConfigSnapshot(),
				newMetaInfo.getStateSerializer());

			if (namespaceCompatibility.isRequiresMigration() || stateCompatibility.isRequiresMigration()) {
				// TODO state migration currently isn't possible.
				throw new StateMigrationException("State migration isn't supported, yet.");
			} else {
				stateInfo.f1 = newMetaInfo;
				return stateInfo.f0;
			}
		}

		byte[] nameBytes = descriptor.getName().getBytes(ConfigConstants.DEFAULT_CHARSET);
		Preconditions.checkState(!Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
			"The chosen state name 'default' collides with the name of the default column family!");

		ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(nameBytes, columnOptions);

		final ColumnFamilyHandle columnFamily;

		try {
			columnFamily = db.createColumnFamily(columnDescriptor);
		} catch (RocksDBException e) {
			throw new IOException("Error creating ColumnFamilyHandle.", e);
		}

		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<N, S>> tuple =
			new Tuple2<>(columnFamily, newMetaInfo);
		Map rawAccess = kvStateInformation;
		rawAccess.put(descriptor.getName(), tuple);
		return columnFamily;
	}

	@Override
	protected <N, T> InternalValueState<N, T> createValueState(
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc, namespaceSerializer);

		return new RocksDBValueState<>(columnFamily, namespaceSerializer,  stateDesc, this);
	}

	@Override
	protected <N, T> InternalListState<N, T> createListState(
		TypeSerializer<N> namespaceSerializer,
		ListStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc, namespaceSerializer);

		return new RocksDBListState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}

	@Override
	protected <N, T> InternalReducingState<N, T> createReducingState(
		TypeSerializer<N> namespaceSerializer,
		ReducingStateDescriptor<T> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc, namespaceSerializer);

		return new RocksDBReducingState<>(columnFamily, namespaceSerializer,  stateDesc, this);
	}

	@Override
	protected <N, T, ACC, R> InternalAggregatingState<N, T, R> createAggregatingState(
		TypeSerializer<N> namespaceSerializer,
		AggregatingStateDescriptor<T, ACC, R> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc, namespaceSerializer);
		return new RocksDBAggregatingState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}

	@Override
	protected <N, T, ACC> InternalFoldingState<N, T, ACC> createFoldingState(
		TypeSerializer<N> namespaceSerializer,
		FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc, namespaceSerializer);

		return new RocksDBFoldingState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}

	@Override
	protected <N, UK, UV> InternalMapState<N, UK, UV> createMapState(
		TypeSerializer<N> namespaceSerializer,
		MapStateDescriptor<UK, UV> stateDesc) throws Exception {

		ColumnFamilyHandle columnFamily = getColumnFamily(stateDesc, namespaceSerializer);

		return new RocksDBMapState<>(columnFamily, namespaceSerializer, stateDesc, this);
	}

	/**
	 * Only visible for testing, DO NOT USE.
	 */
	public File getInstanceBasePath() {
		return instanceBasePath;
	}

	@Override
	public boolean supportsAsynchronousSnapshots() {
		return true;
	}

	@VisibleForTesting
	@SuppressWarnings("unchecked")
	@Override
	public int numStateEntries() {
		int count = 0;

		for (Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> column : kvStateInformation.values()) {
			try (RocksIterator rocksIterator = db.newIterator(column.f0)) {
				rocksIterator.seekToFirst();

				while (rocksIterator.isValid()) {
					count++;
					rocksIterator.next();
				}
			}
		}

		return count;
	}

	/**
	 * Adapter class to bridge between {@link RocksIterator} and {@link Iterator} to iterate over the keys. This class
	 * is not thread safe.
	 *
	 * @param <K> the type of the iterated objects, which are keys in RocksDB.
	 */
	static class RocksIteratorForKeysWrapper<K> implements Iterator<K>, AutoCloseable {
		private final RocksIterator iterator;
		private final String state;
		private final TypeSerializer<K> keySerializer;
		private final int keyGroupPrefixBytes;
		private final byte[] namespaceBytes;
		private final boolean ambiguousKeyPossible;
		private K nextKey;

		RocksIteratorForKeysWrapper(
			RocksIterator iterator,
			String state,
			TypeSerializer<K> keySerializer,
			int keyGroupPrefixBytes,
			boolean ambiguousKeyPossible,
			byte[] namespaceBytes) {
			this.iterator = Preconditions.checkNotNull(iterator);
			this.state = Preconditions.checkNotNull(state);
			this.keySerializer = Preconditions.checkNotNull(keySerializer);
			this.keyGroupPrefixBytes = Preconditions.checkNotNull(keyGroupPrefixBytes);
			this.namespaceBytes = Preconditions.checkNotNull(namespaceBytes);
			this.nextKey = null;
			this.ambiguousKeyPossible = ambiguousKeyPossible;
		}

		@Override
		public boolean hasNext() {
			while (nextKey == null && iterator.isValid()) {
				try {
					byte[] key = iterator.key();
					if (isMatchingNameSpace(key)) {
						ByteArrayInputStreamWithPos inputStream =
							new ByteArrayInputStreamWithPos(key, keyGroupPrefixBytes, key.length - keyGroupPrefixBytes);
						DataInputViewStreamWrapper dataInput = new DataInputViewStreamWrapper(inputStream);
						K value = RocksDBKeySerializationUtils.readKey(
							keySerializer,
							inputStream,
							dataInput,
							ambiguousKeyPossible);
						nextKey = value;
					}
					iterator.next();
				} catch (IOException e) {
					throw new FlinkRuntimeException("Failed to access state [" + state + "]", e);
				}
			}
			return nextKey != null;
		}

		@Override
		public K next() {
			if (!hasNext()) {
				throw new NoSuchElementException("Failed to access state [" + state + "]");
			}

			K tmpKey = nextKey;
			nextKey = null;
			return tmpKey;
		}

		private boolean isMatchingNameSpace(@Nonnull byte[] key) {
			final int namespaceBytesLength = namespaceBytes.length;
			final int basicLength = namespaceBytesLength + keyGroupPrefixBytes;
			if (key.length >= basicLength) {
				for (int i = 1; i <= namespaceBytesLength; ++i) {
					if (key[key.length - i] != namespaceBytes[namespaceBytesLength - i]) {
						return false;
					}
				}
				return true;
			}
			return false;
		}

		@Override
		public void close() {
			iterator.close();
		}
	}
}
