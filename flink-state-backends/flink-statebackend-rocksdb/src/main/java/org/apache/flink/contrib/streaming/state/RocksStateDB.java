///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.contrib.streaming.state;
//
//import org.apache.flink.api.common.state.StateDescriptor;
//import org.apache.flink.api.common.typeutils.CompatibilityResult;
//import org.apache.flink.api.common.typeutils.CompatibilityUtil;
//import org.apache.flink.api.common.typeutils.TypeSerializer;
//import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.ConfigConstants;
//import org.apache.flink.runtime.state.RegisteredKeyedBackendStateMetaInfo;
//import org.apache.flink.util.FileUtils;
//import org.apache.flink.util.IOUtils;
//import org.apache.flink.util.Preconditions;
//import org.apache.flink.util.StateMigrationException;
//
//import org.rocksdb.ColumnFamilyDescriptor;
//import org.rocksdb.ColumnFamilyHandle;
//import org.rocksdb.ColumnFamilyOptions;
//import org.rocksdb.DBOptions;
//import org.rocksdb.RocksDB;
//import org.rocksdb.RocksDBException;
//import org.rocksdb.WriteOptions;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.annotation.Nonnull;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.LinkedHashMap;
//import java.util.Map;
//import java.util.Objects;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//public class RocksStateDB implements AutoCloseable {
//
//	private static final Logger LOG = LoggerFactory.getLogger(RocksDBKeyedStateBackend.class);
//
//	/**
//	 * We are not using the default column family for Flink state ops, but we still need to remember this handle so that
//	 * we can close it properly when the backend is closed. This is required by RocksDB's native memory management.
//	 */
//	@Nonnull
//	private final ColumnFamilyHandle defaultColumnFamily;
//
//	/** The column family options from the options factory. */
//	@Nonnull
//	private final ColumnFamilyOptions columnOptions;
//
//	/** The DB options from the options factory. */
//	@Nonnull
//	private final DBOptions dbOptions;
//
//	/**
//	 * The write options to use in the states. We disable write ahead logging.
//	 */
//	@Nonnull
//	private final WriteOptions writeOptions;
//
//	@Nonnull
//	private final RocksDB db;
//
//	@Nonnull
//	private final LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation;
//
//	@Nonnull
//	private final Map<String, RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredKvStateMetaInfos;
//
//	/** Path where this configured instance stores its data directory. */
//	@Nonnull
//	private final File instanceBasePath;
//
//	@Nonnull
//	private final AtomicBoolean closed;
//
//	public RocksStateDB(
//		@Nonnull ColumnFamilyHandle defaultColumnFamily,
//		@Nonnull ColumnFamilyOptions columnOptions,
//		@Nonnull DBOptions dbOptions,
//		@Nonnull WriteOptions writeOptions,
//		@Nonnull RocksDB db,
//		@Nonnull LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation,
//		@Nonnull Map<String, RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> restoredKvStateMetaInfos,
//		@Nonnull File instanceBasePath,
//		@Nonnull AtomicBoolean closed) {
//
//		this.defaultColumnFamily = defaultColumnFamily;
//		this.columnOptions = columnOptions;
//		this.dbOptions = dbOptions;
//		this.writeOptions = writeOptions;
//		this.db = db;
//		this.kvStateInformation = kvStateInformation;
//		this.restoredKvStateMetaInfos = restoredKvStateMetaInfos;
//		this.instanceBasePath = instanceBasePath;
//		this.closed = closed;
//	}
//
//	@Override
//	public void close() {
//		if (closed.compareAndSet(false, true)) {
//			// RocksDB's native memory management requires that *all* CFs (including default) are closed before the
//			// DB is closed. See:
//			// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
//			// Start with default CF ...
//			IOUtils.closeQuietly(defaultColumnFamily);
//
//			// ... continue with the ones created by Flink...
//			for (Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> columnMetaData :
//				kvStateInformation.values()) {
//				IOUtils.closeQuietly(columnMetaData.f0);
//			}
//
//			// ... and finally close the DB instance ...
//			IOUtils.closeQuietly(db);
//
//			// invalidate the reference
//			//db = null;
//
//			IOUtils.closeQuietly(columnOptions);
//			IOUtils.closeQuietly(dbOptions);
//			IOUtils.closeQuietly(writeOptions);
//			kvStateInformation.clear();
//			restoredKvStateMetaInfos.clear();
//
//			cleanInstanceBasePath();
//		}
//	}
//
//	private void cleanInstanceBasePath() {
//		LOG.info("Deleting existing instance base directory {}.", instanceBasePath);
//
//		try {
//			FileUtils.deleteDirectory(instanceBasePath);
//		} catch (IOException ex) {
//			LOG.warn("Could not delete instance base path for RocksDB: " + instanceBasePath, ex);
//		}
//	}
//
//	/**
//	 * Creates a column family handle for use with a k/v state. When restoring from a snapshot
//	 * we don't restore the individual k/v states, just the global RocksDB database and the
//	 * list of column families. When a k/v state is first requested we check here whether we
//	 * already have a column family for that and return it or create a new one if it doesn't exist.
//	 *
//	 * <p>This also checks whether the {@link StateDescriptor} for a state matches the one
//	 * that we checkpointed, i.e. is already in the map of column families.
//	 */
//	@SuppressWarnings("unchecked")
//	public <N, S> ColumnFamilyHandle getColumnFamily(
//		StateDescriptor<?, S> descriptor, TypeSerializer<N> namespaceSerializer) throws IOException, StateMigrationException {
//
//		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> stateInfo =
//			kvStateInformation.get(descriptor.getName());
//
//		RegisteredKeyedBackendStateMetaInfo<N, S> newMetaInfo = new RegisteredKeyedBackendStateMetaInfo<>(
//			descriptor.getType(),
//			descriptor.getName(),
//			namespaceSerializer,
//			descriptor.getSerializer());
//
//		if (stateInfo != null) {
//			// TODO with eager registration in place, these checks should be moved to restore()
//
//			RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S> restoredMetaInfo =
//				(RegisteredKeyedBackendStateMetaInfo.Snapshot<N, S>) restoredKvStateMetaInfos.get(descriptor.getName());
//
//			Preconditions.checkState(
//				Objects.equals(newMetaInfo.getName(), restoredMetaInfo.getName()),
//				"Incompatible state names. " +
//					"Was [" + restoredMetaInfo.getName() + "], " +
//					"registered with [" + newMetaInfo.getName() + "].");
//
//			if (!Objects.equals(newMetaInfo.getStateType(), StateDescriptor.Type.UNKNOWN)
//				&& !Objects.equals(restoredMetaInfo.getStateType(), StateDescriptor.Type.UNKNOWN)) {
//
//				Preconditions.checkState(
//					newMetaInfo.getStateType() == restoredMetaInfo.getStateType(),
//					"Incompatible state types. " +
//						"Was [" + restoredMetaInfo.getStateType() + "], " +
//						"registered with [" + newMetaInfo.getStateType() + "].");
//			}
//
//			// check compatibility results to determine if state migration is required
//			CompatibilityResult<N> namespaceCompatibility = CompatibilityUtil.resolveCompatibilityResult(
//				restoredMetaInfo.getNamespaceSerializer(),
//				null,
//				restoredMetaInfo.getNamespaceSerializerConfigSnapshot(),
//				newMetaInfo.getNamespaceSerializer());
//
//			CompatibilityResult<S> stateCompatibility = CompatibilityUtil.resolveCompatibilityResult(
//				restoredMetaInfo.getStateSerializer(),
//				UnloadableDummyTypeSerializer.class,
//				restoredMetaInfo.getStateSerializerConfigSnapshot(),
//				newMetaInfo.getStateSerializer());
//
//			if (namespaceCompatibility.isRequiresMigration() || stateCompatibility.isRequiresMigration()) {
//				// TODO state migration currently isn't possible.
//				throw new StateMigrationException("State migration isn't supported, yet.");
//			} else {
//				stateInfo.f1 = newMetaInfo;
//				return stateInfo.f0;
//			}
//		}
//
//		byte[] nameBytes = descriptor.getName().getBytes(ConfigConstants.DEFAULT_CHARSET);
//		Preconditions.checkState(!Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
//			"The chosen state name 'default' collides with the name of the default column family!");
//
//		ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(nameBytes, columnOptions);
//
//		final ColumnFamilyHandle columnFamily;
//
//		try {
//			columnFamily = db.createColumnFamily(columnDescriptor);
//		} catch (RocksDBException e) {
//			throw new IOException("Error creating ColumnFamilyHandle.", e);
//		}
//
//		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> tuple =
//			new Tuple2<>(columnFamily, newMetaInfo);
//		kvStateInformation.put(descriptor.getName(), tuple);
//
//		return columnFamily;
//	}
//}
