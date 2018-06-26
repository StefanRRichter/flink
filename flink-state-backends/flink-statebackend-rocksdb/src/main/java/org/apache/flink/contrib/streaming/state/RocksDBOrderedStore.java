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
import org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * @param <T>
 */
public class RocksDBOrderedStore<T> implements CachingInternalPriorityQueueSet.OrderedSetStore<T> {

	private static final byte[] DUMMY_BYTES = "0".getBytes(ConfigConstants.DEFAULT_CHARSET);

	private final RocksDB db;
	private final ColumnFamilyHandle columnFamilyHandle;
	private final ReadOptions readOptions;
	private final TypeSerializer<T> serializer;
	private final ByteArrayOutputStreamWithPos outputStream;
	private final DataOutputViewStreamWrapper outputView;
	private final RocksDBWriteBatchWrapper batchWrapper;
	private final byte[] groupPrefixBytes;
	private final int keyGroupId;

	public RocksDBOrderedStore(
		int keyGroupId,
		RocksDB db,
		ColumnFamilyHandle columnFamilyHandle,
		ReadOptions readOptions,
		TypeSerializer<T> serializer,
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputViewStreamWrapper outputView,
		RocksDBWriteBatchWrapper batchWrapper) {

		this.db = db;
		this.columnFamilyHandle = columnFamilyHandle;
		this.readOptions = readOptions;
		this.serializer = serializer;
		this.outputStream = outputStream;
		this.outputView = outputView;
		this.keyGroupId = keyGroupId;
		this.batchWrapper = batchWrapper;
		this.groupPrefixBytes = createKeyGroupBytes(keyGroupId);
	}

	private byte[] createKeyGroupBytes(int keyGroupId) {

		outputStream.reset();

		try {
			outputView.writeShort(keyGroupId);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not write key-group bytes.", e);
		}

		return outputStream.toByteArray();
	}

	@Override
	public void add(T element) {
		byte[] timerBytes = serializeTimer(element);
		try {
			batchWrapper.put(columnFamilyHandle, timerBytes, DUMMY_BYTES);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error while getting timer from RocksDB.", e);
		}
	}

	@Override
	public void remove(T element) {
		byte[] timerBytes = serializeTimer(element);
		try {
			batchWrapper.remove(columnFamilyHandle, timerBytes);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error while removing timer from RocksDB.", e);
		}
	}

	@Override
	public int size() {

		flushWriteBatch();

		int count = 0;
		try (RocksIteratorWrapper iter = new RocksIteratorWrapper(db.newIterator(columnFamilyHandle, readOptions))) {
			iter.seek(groupPrefixBytes);
			while (iter.isValid() && isPrefixWith(iter.key(), groupPrefixBytes)) {
				++count;
				iter.next();
			}
		}
		return count;
	}

	@Override
	public CloseableIterator<T> orderedIterator() {

		flushWriteBatch();

		return new RocksToJavaIteratorAdapter(
			new RocksIteratorWrapper(
				db.newIterator(columnFamilyHandle, readOptions)));
	}

	/**
	 * Ensures that recent writes are flushed and reflect in the database.
	 */
	private void flushWriteBatch() {
		try {
			batchWrapper.flush();
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException(e);
		}
	}

	private static boolean isPrefixWith(byte[] bytes, byte[] prefixBytes) {
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

	/**
	 * Adapter between RocksDB iterator and Java iterator. This is also closeable to release the native resources after
	 * use.
	 */
	private class RocksToJavaIteratorAdapter implements CloseableIterator<T> {

		@Nonnull
		private final RocksIteratorWrapper iterator;

		@Nullable
		private T currentElement;

		private RocksToJavaIteratorAdapter(@Nonnull RocksIteratorWrapper iterator) {
			this.iterator = iterator;
			try {
				iterator.seek(groupPrefixBytes);
				deserializeNextElementIfAvailable();
			} catch (Exception ex) {
				// ensure resource cleanup also in the face of (runtime) exceptions in the constructor.
				iterator.close();
				throw new FlinkRuntimeException("Could not initialize ordered iterator.", ex);
			}
		}

		@Override
		public void close() {
			iterator.close();
		}

		@Override
		public boolean hasNext() {
			return currentElement != null;
		}

		@Override
		public T next() {
			final T returnElement = this.currentElement;
			if (returnElement == null) {
				throw new NoSuchElementException("Iterator has no more elements!");
			}
			iterator.next();
			deserializeNextElementIfAvailable();
			return returnElement;
		}

		private void deserializeNextElementIfAvailable() {
			if (iterator.isValid()) {
				final byte[] elementBytes = iterator.key();
				if (isPrefixWith(elementBytes, groupPrefixBytes)) {
					this.currentElement = deserializeTimer(elementBytes);
				} else {
					this.currentElement = null;
				}
			} else {
				this.currentElement = null;
			}
		}
	}
}
