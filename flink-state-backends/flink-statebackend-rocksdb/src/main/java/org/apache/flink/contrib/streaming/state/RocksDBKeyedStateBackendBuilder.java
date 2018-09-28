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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.restore.RocksDBFullRestoreOperation;
import org.apache.flink.contrib.streaming.state.restore.RocksDBIncrementalRestoreOperation;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FileUtils;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class RocksDBKeyedStateBackendBuilder<K> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBKeyedStateBackendBuilder.class);
	private static final String DB_INSTANCE_DIR_STRING = "db";

	private final String operatorIdentifier;

	private final ClassLoader userCodeClassLoader;

	private final TypeSerializer<K> keySerializer;

	private final TaskKvStateRegistry kvStateRegistry;

	private final int numberOfKeyGroups;
	private final KeyGroupRange keyGroupRange;

	private final boolean enableIncrementalCheckpointing;
	private final RocksDBStateBackend.PriorityQueueStateType priorityQueueStateType;
	private final LocalRecoveryConfig localRecoveryConfig;
	private final ExecutionConfig executionConfig;
	private final TtlTimeProvider ttlTimeProvider;

	//--------------

	/** The column family options from the options factory. */
	private final ColumnFamilyOptions columnFamilyOptions;

	/** The DB options from the options factory. */
	private final DBOptions dbOptions;

	/** Path where this configured instance stores its data directory. */
	private final File instanceBasePath;

	/** Path where this configured instance stores its RocksDB database. */
	private final File instanceRocksDBPath;

	/** The write options to use in the states. We disable write ahead logging. */
	private final WriteOptions writeOptions;

//	/** Factory for priority queue state. */
//	private final PriorityQueueSetFactory priorityQueueFactory;

	/**
	 * Information about the k/v states, maintained in the order as we create them. This is used to retrieve the
	 * column family that is used for a state and also for sanity checks when restoring.
	 */
	private final LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase>> kvStateInformation;

	/**
	 * Map of state names to their corresponding restored state meta info.
	 *
	 * <p>TODO this map can be removed when eager-state registration is in place.
	 * TODO we currently need this cached to check state migration strategies when new serializers are registered.
	 */
	private final Map<String, StateMetaInfoSnapshot> restoredKvStateMetaInfos;

	@Nonnull
	private Collection<KeyedStateHandle> restoreStateHandles;

	@Nullable
	private RocksDB db;

	public RocksDBKeyedStateBackendBuilder(
		String operatorIdentifier,
		ClassLoader userCodeClassLoader,
		TypeSerializer<K> keySerializer,
		TaskKvStateRegistry kvStateRegistry,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		boolean enableIncrementalCheckpointing,
		RocksDBStateBackend.PriorityQueueStateType priorityQueueStateType,
		LocalRecoveryConfig localRecoveryConfig,
		ExecutionConfig executionConfig,
		TtlTimeProvider ttlTimeProvider,
		ColumnFamilyOptions columnFamilyOptions,
		DBOptions dbOptions,
		File instanceBasePath) {

		this.operatorIdentifier = operatorIdentifier;
		this.userCodeClassLoader = userCodeClassLoader;
		this.keySerializer = keySerializer;
		this.kvStateRegistry = kvStateRegistry;
		this.numberOfKeyGroups = numberOfKeyGroups;
		this.keyGroupRange = keyGroupRange;
		this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
		this.priorityQueueStateType = priorityQueueStateType;
		this.localRecoveryConfig = localRecoveryConfig;
		this.executionConfig = executionConfig;
		this.ttlTimeProvider = ttlTimeProvider;
		this.columnFamilyOptions = columnFamilyOptions;
		this.dbOptions = dbOptions;
		this.instanceBasePath = instanceBasePath;

		this.restoreStateHandles = Collections.emptyList();
		this.kvStateInformation = new LinkedHashMap<>();
		this.restoredKvStateMetaInfos = new HashMap<>();
		this.writeOptions = new WriteOptions().setDisableWAL(true);
		this.instanceRocksDBPath = new File(instanceBasePath, DB_INSTANCE_DIR_STRING);
//		switch (priorityQueueStateType) {
//			case HEAP:
//				this.priorityQueueFactory = new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);
//				break;
//			case ROCKSDB:
//				this.priorityQueueFactory = new RocksDBKeyedStateBackend.RocksDBPriorityQueueSetFactory();
//				break;
//			default:
//				throw new IllegalArgumentException("Unknown priority queue state type: " + priorityQueueStateType);
//		}
	}

	private void prepareDirectories() throws IOException {
		checkAndCreateDirectory(instanceBasePath);
		if (instanceRocksDBPath.exists()) {
			// Clear the base directory when the backend is created
			// in case something crashed and the backend never reached dispose()
			FileUtils.deleteDirectory(instanceBasePath);
		}
	}

	public void setRestoreStateHandles(@Nonnull Collection<KeyedStateHandle> restoreStateHandles) {
		this.restoreStateHandles = restoreStateHandles;
	}

	private static void checkAndCreateDirectory(File directory) throws IOException {
		if (directory.exists()) {
			if (!directory.isDirectory()) {
				throw new IOException("Not a directory: " + directory);
			}
		} else if (!directory.mkdirs()) {
			throw new IOException(String.format("Could not create RocksDB data directory at %s.", directory));
		}
	}

	private void restore(Collection<KeyedStateHandle> restoreState) throws Exception {

		LOG.info("Initializing RocksDB keyed state backend.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Restoring snapshot from state handles: {}.", restoreState);
		}

		// clear all meta data
		kvStateInformation.clear();
		restoredKvStateMetaInfos.clear();

		try {
			RocksDBIncrementalRestoreOperation<K> incrementalRestoreOperation = null;
			if (restoreState == null || restoreState.isEmpty()) {
				createDB();
			} else {
				KeyedStateHandle firstStateHandle = restoreState.iterator().next();
				if (firstStateHandle instanceof IncrementalKeyedStateHandle
					|| firstStateHandle instanceof IncrementalLocalKeyedStateHandle) {
					incrementalRestoreOperation = new RocksDBIncrementalRestoreOperation<>(this);
					incrementalRestoreOperation.restore(restoreState);
				} else {
					RocksDBFullRestoreOperation<K> fullRestoreOperation = new RocksDBFullRestoreOperation<>(this);
					fullRestoreOperation.doRestore(restoreState);
				}
			}

			initializeSnapshotStrategy(incrementalRestoreOperation);
		} catch (Exception ex) {
			dispose();
			throw ex;
		}
	}


	public RocksDBKeyedStateBackend<K> build() throws IOException {

		prepareDirectories();

		if (!restoreStateHandles.isEmpty()) {

		}

		return new RocksDBKeyedStateBackend<>();
	}
}
