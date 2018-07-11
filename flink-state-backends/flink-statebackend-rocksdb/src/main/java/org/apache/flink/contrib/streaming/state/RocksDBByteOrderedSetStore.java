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

import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * Dummy comment.
 */
public class RocksDBByteOrderedSetStore implements CachingInternalPriorityQueueSet.OrderedSetStore<byte[]> {

	/** Serialized empty value to insert into RocksDB. */
	private static final byte[] DUMMY_BYTES = new byte[] {0};

	/** The RocksDB instance that serves as store. */
	@Nonnull
	private final RocksDB db;

	/** Handle to the column family of the RocksDB instance in which the elements are stored. */
	@Nonnull
	private final ColumnFamilyHandle columnFamilyHandle;

	/** Wrapper to batch all writes to RocksDB. */
	@Nonnull
	private final RocksDBWriteBatchWrapper batchWrapper;

	/** The key-group id in serialized form. */
	@Nonnull
	private final byte[] groupPrefixBytes;

	public RocksDBByteOrderedSetStore(
		@Nonnegative int keyGroupId,
		@Nonnegative int keyGroupPrefixBytes,
		@Nonnull RocksDB db,
		@Nonnull ColumnFamilyHandle columnFamilyHandle,
		@Nonnull RocksDBWriteBatchWrapper batchWrapper) {
		this.db = db;
		this.columnFamilyHandle = columnFamilyHandle;
		this.batchWrapper = batchWrapper;
		this.groupPrefixBytes = createKeyGroupBytes(keyGroupId, keyGroupPrefixBytes);
	}

	@Override
	public void add(@Nonnull byte[] element) {
		try {
			batchWrapper.put(columnFamilyHandle, element, DUMMY_BYTES);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Exception while interacting with RocksDB.", e);
		}
	}

	@Override
	public void remove(@Nonnull byte[] element) {
		try {
			batchWrapper.remove(columnFamilyHandle, element);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Exception while interacting with RocksDB.", e);
		}
	}

	@Override
	public int size() {
		return 0;
	}

	@Nonnull
	@Override
	public CloseableIterator<byte[]> orderedIterator() {
		return null;
	}

	private byte[] createKeyGroupBytes(int keyGroupId, int numPrefixBytes) {

		ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos(8);
		DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);

		try {
			RocksDBKeySerializationUtils.writeKeyGroup(keyGroupId, numPrefixBytes, outputView);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not write key-group bytes.", e);
		}

		return outputStream.toByteArray();
	}
}
