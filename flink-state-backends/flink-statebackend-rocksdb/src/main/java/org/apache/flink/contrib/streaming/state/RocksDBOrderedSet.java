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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.heap.TreeCachingOrderedSetPartition;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.util.Comparator;

public class RocksDBOrderedSet<T> extends TreeCachingOrderedSetPartition<T> {

	private static final byte[] DUMMY_BYTES = "0".getBytes(ConfigConstants.DEFAULT_CHARSET);

	private final RocksDB db;
	private final ColumnFamilyHandle columnFamilyHandle;
	private final WriteOptions writeOptions;
	private final ReadOptions readOptions;
	private final TypeSerializer<T> serializer;

	private final ByteArrayOutputStreamWithPos outputStream;
	private final DataOutputViewStreamWrapper outputView;
//	private final ByteArrayInputStreamWithPos inputStream;
//	private final DataInputViewStreamWrapper inputView;

	private final byte[] groupPrefixBytes;

	public RocksDBOrderedSet(
		int keyGroupId,
		Comparator<T> elementComparator,
		int capacity,
		RocksDB db,
		ColumnFamilyHandle columnFamilyHandle,
		WriteOptions writeOptions,
		ReadOptions readOptions,
		TypeSerializer<T> serializer,
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputViewStreamWrapper outputView) {
//		ByteArrayInputStreamWithPos inputStream,
//		DataInputViewStreamWrapper inputView

		super(keyGroupId, elementComparator, capacity);
		this.db = db;
		this.columnFamilyHandle = columnFamilyHandle;
		this.writeOptions = writeOptions;
		this.readOptions = readOptions;
		this.serializer = serializer;
		this.outputStream = outputStream;
		this.outputView = outputView;
//		this.inputStream = inputStream;
//		this.inputView = inputView;
		this.groupPrefixBytes = createKeyGroupBytes();
	}

	private byte[] createKeyGroupBytes() {

		outputStream.reset();

		try {
			outputView.writeShort(keyGroupId);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not write key-group bytes.", e);
		}

		return outputStream.toByteArray();
	}

//	/**
//	 * Function to extract the key from contained elements.
//	 */
//	private final KeyExtractorFunction<T> keyExtractor;
//	private final int totalKeyGroups;
//	private final int firstKeyGroup;

	@Override
	protected void addToBackend(T element) {
		byte[] timerBytes = serializeTimer(element);
		try {
			db.put(columnFamilyHandle, writeOptions, timerBytes, DUMMY_BYTES);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error while getting timer from RocksDB.", e);
		}
	}

	@Override
	protected void removeFromBackend(T element) {
		byte[] timerBytes = serializeTimer(element);
		try {
			db.delete(columnFamilyHandle, writeOptions, timerBytes);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error while removing timer from RocksDB.", e);
		}
	}

	@Override
	protected void refillCacheFromBackend() {
		try (RocksIteratorWrapper iterator = new RocksIteratorWrapper(db.newIterator(columnFamilyHandle, readOptions))) {

			iterator.seek(groupPrefixBytes);

			while (iterator.isValid()) {

				byte[] elementBytes = iterator.key();

				if (!isPrefixWith(elementBytes, groupPrefixBytes)) {
					break;
				}

				addToCache(deserializeTimer(elementBytes));

				if (isCacheFull()) {
					break;
				}

				iterator.next();
			}
		}
	}

	@Override
	protected int size() {
		return 0;
	}

	private static boolean isPrefixWith(byte[] bytes, byte[] prefixBytes) {
//		if (bytes.length < prefixBytes.length) {
//			return false;
//		}

		for (int i = 0; i < prefixBytes.length; ++i) {
			if (bytes[i] != prefixBytes[i]) {
				return false;
			}
		}
		return true;
	}

	private byte[] serializeTimer(T element) {
		try {
			outputStream.reset();
			outputView.writeShort(keyGroupId);
			serializer.serialize(element, outputView);
			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing the timer.", e);
		}
	}

	private T deserializeTimer(byte[] bytes) {
		try {
			ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(bytes);
			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
			inputView.readShort();
			return serializer.deserialize(inputView);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while deserializing the timer.", e);
		}
	}
}
