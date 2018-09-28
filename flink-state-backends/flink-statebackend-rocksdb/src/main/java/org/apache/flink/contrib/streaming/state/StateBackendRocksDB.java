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

import org.apache.flink.util.FileUtils;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * TODO
 */
public class StateBackendRocksDB implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(StateBackendRocksDB.class);

	@Nonnull
	private final File backendInstanceBasePath;

	@Nonnull
	private final DBOptions dbOptions;

	@Nonnull
	private final ColumnFamilyOptions columnFamilyOptions;

	@Nonnull
	private final WriteOptions writeOptions;

	@Nonnull
	private final ResourceGuard resourceGuard;

	@Nonnull
	private final StateRocksDB stateRocksDB;

	public StateBackendRocksDB(
		@Nonnull File backendInstanceBasePath,
		@Nonnull DBOptions dbOptions,
		@Nonnull ColumnFamilyOptions columnFamilyOptions,
		@Nonnull WriteOptions writeOptions,
		@Nonnull ResourceGuard resourceGuard,
		@Nonnull StateRocksDB stateRocksDB) {

		this.backendInstanceBasePath = backendInstanceBasePath;
		this.dbOptions = dbOptions;
		this.columnFamilyOptions = columnFamilyOptions;
		this.writeOptions = writeOptions;
		this.resourceGuard = resourceGuard;
		this.stateRocksDB = stateRocksDB;
	}

	@Nonnull
	public DBOptions getDbOptions() {
		return dbOptions;
	}

	@Nonnull
	public ColumnFamilyOptions getColumnFamilyOptions() {
		return columnFamilyOptions;
	}

	@Nonnull
	public WriteOptions getWriteOptions() {
		return writeOptions;
	}

	@Nonnull
	public ResourceGuard getResourceGuard() {
		return resourceGuard;
	}

	@Nonnull
	public StateRocksDB getStateRocksDB() {
		return stateRocksDB;
	}

	//	@Nonnull
//	public StateColumnFamilyHandle createNewStateColumnFamily(@Nonnull RegisteredStateMetaInfoBase stateMetaInfo) {
//		return createNewStateColumnFamily(stateMetaInfo, columnFamilyOptions);
//	}
//
//	@Nullable
//	public StateColumnFamilyHandle getStateColumnFamily(@Nonnull String stateColumnFamilyName) {
//		return stateRocksDB.getStateColumnFamily(stateColumnFamilyName);
//	}
//
//	@Nonnull
//	public StateColumnFamilyHandle createNewStateColumnFamily(
//		@Nonnull RegisteredStateMetaInfoBase stateMetaInfo,
//		@Nonnull ColumnFamilyOptions columnFamilyOptions) {
//		return stateRocksDB.createNewStateColumnFamily(stateMetaInfo, columnFamilyOptions);
//	}
//
//	public void updateStateColumnFamilyMetaInfo(@Nonnull RegisteredStateMetaInfoBase stateMetaInfo) {
//		stateRocksDB.updateStateColumnFamilyMetaInfo(stateMetaInfo);
//	}

	@Override
	public void close() {

		if (!resourceGuard.isClosed()) {
			// This call will block until all clients that still acquire access to the RocksDB instance have released
			// it, so that we cannot release the native resources while clients are still working with it in parallel.
			resourceGuard.close();

			stateRocksDB.close();

			writeOptions.close();
			columnFamilyOptions.close();
			dbOptions.close();

			deleteDBInstanceDirectory();
		}
	}

	private void deleteDBInstanceDirectory() {
		LOG.debug("Deleting StateBackendRocksDB base directory {}.", backendInstanceBasePath);
		try {
			FileUtils.deleteDirectory(backendInstanceBasePath);
		} catch (IOException ex) {
			LOG.warn("Could not delete StateBackendRocksDB base directory in resource cleanup: "
				+ backendInstanceBasePath, ex);
		}
	}
}
