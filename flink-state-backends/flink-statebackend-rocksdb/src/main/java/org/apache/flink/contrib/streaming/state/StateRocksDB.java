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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * A RocksDB instance that can be used to manage State in Flink.
 */
public class StateRocksDB implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(StateRocksDB.class);
	private static final String DEFAULT_CF_NAME_STRING =
		new String(RocksDB.DEFAULT_COLUMN_FAMILY, ConfigConstants.DEFAULT_CHARSET);

	/** Working directory of this db instance. */
	@Nonnull
	private final File dbInstancePath;

	/**
	 * Information about the k/v states, maintained in the order as we create them. This is used to retrieve the
	 * column family that is used for a state and also for sanity checks when restoring.
	 */
	@Nonnull
	private final LinkedHashMap<String, StateColumnFamilyHandle> stateColumnFamiliesByName;

	/**
	 * Our RocksDB database, this is used by the actual subclasses of {@link AbstractRocksDBState}
	 * to store state. The different k/v states that we have don't each have their own RocksDB
	 * instance. They all write to this instance but to their own column family.
	 */
	@Nonnull
	private final RocksDB rocksDB;

	public StateRocksDB(
		@Nonnull File dbInstancePath,
		@Nonnull RocksDB rocksDB,
		@Nonnull LinkedHashMap<String, StateColumnFamilyHandle> stateColumnFamiliesByName) {
		this.dbInstancePath = dbInstancePath;
		this.rocksDB = rocksDB;
		this.stateColumnFamiliesByName = stateColumnFamiliesByName;
	}

	@Override
	public void close() {

		// RocksDB's native memory management requires that *all* CFs (including default) are closed before the
		// DB is closed. See:
		// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
		// Start with default CF ...
		rocksDB.getDefaultColumnFamily().close();

		// ... continue with the ones created by Flink...
		for (StateColumnFamilyHandle columnMetaData : stateColumnFamiliesByName.values()) {
			columnMetaData.getColumnFamilyHandle().close();
		}

		// ... and finally close the DB instance ...
		rocksDB.close();

		stateColumnFamiliesByName.clear();

		deleteDBInstanceDirectory();
	}


	@Nonnull
	private static ColumnFamilyDescriptor createColumnFamilyDescriptor(
		@Nonnull String cfName,
		@Nonnull ColumnFamilyOptions cfOptions) {
		return new ColumnFamilyDescriptor(cfName.getBytes(ConfigConstants.DEFAULT_CHARSET), cfOptions);
	}

	@Nullable
	public StateColumnFamilyHandle getStateColumnFamily(@Nonnull String stateColumnFamilyName) {
		return stateColumnFamiliesByName.get(stateColumnFamilyName);
	}

	@Nonnull
	public StateColumnFamilyHandle createNewStateColumnFamily(
		@Nonnull RegisteredStateMetaInfoBase stateMetaInfo,
		@Nonnull ColumnFamilyOptions columnFamilyOptions) {

		final String stateName = stateMetaInfo.getName();

		if (DEFAULT_CF_NAME_STRING.equals(stateName)) {
			throw new IllegalStateException(
				"The chosen state name " + stateName + " collides with the name of the default column family!");
		}

		if (stateColumnFamiliesByName.get(stateName) != null) {
			throw new IllegalStateException(
				"State with name " + stateName + " already exists!");
		}

		ColumnFamilyDescriptor columnDescriptor = createColumnFamilyDescriptor(stateName, columnFamilyOptions);

		try {
			ColumnFamilyHandle columnFamilyHandle = rocksDB.createColumnFamily(columnDescriptor);
			return new StateColumnFamilyHandle(columnFamilyHandle, stateMetaInfo);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error when creating ColumnFamilyHandle for state " + stateName, e);
		}
	}

	public void updateStateColumnFamilyMetaInfo(@Nonnull RegisteredStateMetaInfoBase stateMetaInfo) {
		final String stateName = stateMetaInfo.getName();
		final StateColumnFamilyHandle stateColumnFamilyHandle = stateColumnFamiliesByName.get(stateName);
		if (stateColumnFamilyHandle == null) {
			throw new IllegalStateException("State with name " + stateName + " does not exists.");
		}
		stateColumnFamilyHandle.setStateMetaInfo(stateMetaInfo);
	}

	@Nonnull
	public RocksIteratorWrapper createSafeRocksIterator(
		@Nonnull ColumnFamilyHandle columnFamilyHandle,
		@Nonnull ReadOptions readOptions) {
		return new RocksIteratorWrapper(rocksDB.newIterator(columnFamilyHandle, readOptions));
	}


	@Nonnull
	public RocksIteratorWrapper createSafeRocksIterator(@Nonnull ColumnFamilyHandle columnFamilyHandle) {
		return new RocksIteratorWrapper(rocksDB.newIterator(columnFamilyHandle));
	}

	@Nonnull
	public RocksDBWriteBatchWrapper createSafeWriteBatch() {
		return new RocksDBWriteBatchWrapper(rocksDB);
	}

	@Nonnull
	public RocksDB getRocksDB() {
		return rocksDB;
	}

	@Deprecated
	@Nonnull
	public LinkedHashMap<String, StateColumnFamilyHandle> getStateColumnFamiliesByName() {
		return stateColumnFamiliesByName;
	}

	@Nonnull
	public static StateRocksDB createStateRocksDB(
		@Nonnull DBOptions dbOptions,
		@Nonnull ColumnFamilyOptions cfOptions,
		@Nonnull File dbInstancePath,
		@Nonnull List<RegisteredStateMetaInfoBase> initialStateColumnMetaInfo) throws Exception {

		final LinkedHashSet<String> nameDeDuplicationSet = new LinkedHashSet<>(initialStateColumnMetaInfo.size());
		for (RegisteredStateMetaInfoBase stateMetaInfoBase : initialStateColumnMetaInfo) {
			String stateColumnFamilyName = stateMetaInfoBase.getName();
			boolean alreadyContained = nameDeDuplicationSet.add(stateColumnFamilyName);
			Preconditions.checkState(!alreadyContained, "Found duplicate column family name in meta information.");
		}

		Preconditions.checkState(
			!nameDeDuplicationSet.contains(DEFAULT_CF_NAME_STRING),
			"Found meta information that requested name that collides with the default column family.");

		final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			new ArrayList<>(1 + nameDeDuplicationSet.size());

		// We add the required descriptor for the default CF in FIRST position, see
		// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));

		for (String columnFamilyName : nameDeDuplicationSet) {
			columnFamilyDescriptors.add(createColumnFamilyDescriptor(columnFamilyName, cfOptions));
		}

		final List<ColumnFamilyHandle> createdColumnFamilyHandlesOut = new ArrayList<>(columnFamilyDescriptors.size());

		RocksDB createdDB = null;
		RocksDBWriteBatchWrapper writeBatchWrapper = null;

		try {
			createdDB = RocksDB.open(
				dbOptions,
				dbInstancePath.getAbsolutePath(),
				columnFamilyDescriptors,
				createdColumnFamilyHandlesOut);

			// requested + default CF
			Preconditions.checkState(
				columnFamilyDescriptors.size() == createdColumnFamilyHandlesOut.size(),
				"Unexpected number of created column family handles.");

			final LinkedHashMap<String, StateColumnFamilyHandle> stateColumnFamiliesByName =
				new LinkedHashMap<>(initialStateColumnMetaInfo.size());

			for (int i = 0; i < initialStateColumnMetaInfo.size(); ++i) {
				RegisteredStateMetaInfoBase stateMetaInfo = initialStateColumnMetaInfo.get(i);
				ColumnFamilyHandle columnFamilyHandle = createdColumnFamilyHandlesOut.get(i + 1);
				StateColumnFamilyHandle stateColumnFamilyHandle =
					new StateColumnFamilyHandle(columnFamilyHandle, stateMetaInfo);
				stateColumnFamiliesByName.put(stateMetaInfo.getName(), stateColumnFamilyHandle);
			}

			return new StateRocksDB(
				dbInstancePath,
				createdDB,
				stateColumnFamiliesByName);

		} catch (Exception ex) {

			// Resource cleanup in reversed order of creation
			IOUtils.closeQuietly(writeBatchWrapper);
			for (ColumnFamilyHandle columnFamilyHandle : createdColumnFamilyHandlesOut) {
				IOUtils.closeQuietly(columnFamilyHandle);
			}
			IOUtils.closeQuietly(createdDB);

			throw ex;
		}
	}

	private void deleteDBInstanceDirectory() {
		LOG.debug("Deleting StateRocksDB instance directory {}.", dbInstancePath);
		try {
			FileUtils.deleteDirectory(dbInstancePath);
		} catch (IOException ex) {
			LOG.warn("Could not delete StateRocksDB instance directory resource cleanup: " + dbInstancePath, ex);
		}
	}
}
