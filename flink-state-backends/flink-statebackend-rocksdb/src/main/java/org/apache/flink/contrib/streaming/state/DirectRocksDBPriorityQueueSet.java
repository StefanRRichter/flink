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
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.primitives.UnsignedBytes;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class DirectRocksDBPriorityQueueSet<T> implements InternalPriorityQueue<T>, HeapPriorityQueueElement {

	/** Serialized empty value to insert into RocksDB. */
	private static final byte[] DUMMY_BYTES = new byte[] {0};

	/** Comparator for byte arrays. */
	private static final Comparator<byte[]> LEXICOGRAPHICAL_COMPARATOR = UnsignedBytes.lexicographicalComparator();

	/** The RocksDB instance that serves as store. */
	@Nonnull
	private final RocksDB db;

	/** Handle to the column family of the RocksDB instance in which the elements are stored. */
	@Nonnull
	private final ColumnFamilyHandle columnFamilyHandle;

	/**
	 * Serializer for the contained elements. The lexicographical order of the bytes of serialized objects must be
	 * aligned with their logical order.
	 */
	@Nonnull
	private final TypeSerializer<T> byteOrderProducingSerializer;

	/** Wrapper to batch all writes to RocksDB. */
	@Nonnull
	private final WriteOptions writeOptions;

	/** The key-group id in serialized form. */
	@Nonnull
	private final byte[] groupPrefixBytes;

	/** Output stream that helps to serialize elements. */
	@Nonnull
	private final ByteArrayOutputStreamWithPos outputStream;

	/** Output view that helps to serialize elements, must wrap the output stream. */
	@Nonnull
	private final DataOutputViewStreamWrapper outputView;

	@Nullable
	private T headElement;

	@Nonnull
	private byte[] latestHeadElementBytes;

	private int internalIndex;

	public DirectRocksDBPriorityQueueSet(
		@Nonnegative int keyGroupId,
		@Nonnegative int keyGroupPrefixBytes,
		@Nonnull RocksDB db,
		@Nonnull ColumnFamilyHandle columnFamilyHandle,
		@Nonnull TypeSerializer<T> byteOrderProducingSerializer,
		@Nonnull ByteArrayOutputStreamWithPos outputStream,
		@Nonnull DataOutputViewStreamWrapper outputView,
		@Nonnull RocksDBWriteBatchWrapper batchWrapper) {
		this.db = db;
		this.columnFamilyHandle = columnFamilyHandle;
		this.byteOrderProducingSerializer = byteOrderProducingSerializer;
		this.outputStream = outputStream;
		this.outputView = outputView;
		this.writeOptions = batchWrapper.getOptions();
		this.groupPrefixBytes = createKeyGroupBytes(keyGroupId, keyGroupPrefixBytes);
		this.latestHeadElementBytes = groupPrefixBytes;
		this.internalIndex = HeapPriorityQueueElement.NOT_CONTAINED;
		this.headElement = null;
	}

	private byte[] createKeyGroupBytes(int keyGroupId, int numPrefixBytes) {

		outputStream.reset();

		try {
			RocksDBKeySerializationUtils.writeKeyGroup(keyGroupId, numPrefixBytes, outputView);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not write key-group bytes.", e);
		}

		return outputStream.toByteArray();
	}

	@Override
	public void bulkPoll(@Nonnull Predicate<T> canConsume, @Nonnull Consumer<T> consumer) {
		final T startHead = this.headElement;
		if (!isEmpty() && canConsume.test(startHead)) {
			try (final RocksToJavaIteratorAdapter iterator = iterator()) {
				while (iterator.hasNext()) {
					final byte[] nextKeyBytes = iterator.getNextElementBytes();
					final T next = iterator.next();
					if (canConsume.test(next)) {
						db.delete(columnFamilyHandle, writeOptions, nextKeyBytes);
						consumer.accept(next);
					} else {
						if (LEXICOGRAPHICAL_COMPARATOR.compare(nextKeyBytes, latestHeadElementBytes) < 0) {
							latestHeadElementBytes = nextKeyBytes;
							this.headElement = next;
							return;
						}
					}
				}
				if (startHead == headElement) { // TODO this is not quiet enough, could be reinserting the exact same!!!
					this.headElement = null;
				}
			} catch (Exception ex) {
				throw new FlinkRuntimeException(ex);
			}
		}
	}

	@Nullable
	@Override
	public T poll() {
		T toReturn = headElement;
		if (toReturn != null) {
			try {
				db.delete(columnFamilyHandle, writeOptions, latestHeadElementBytes);
			} catch (RocksDBException e) {
				throw new FlinkRuntimeException(e);
			}
			updateHead();
		}
		return toReturn;
	}

	@Nullable
	@Override
	public T peek() {
		return headElement;
	}

	@Override
	public boolean add(@Nonnull T toAdd) {
		final byte[] elementBytes = serializeElement(toAdd);

		final int compareWithHeadResult = !isEmpty() ?
			LEXICOGRAPHICAL_COMPARATOR.compare(elementBytes, latestHeadElementBytes) : -1;

		try {
			switch (Integer.signum(compareWithHeadResult)) {
				case -1:
					headElement = toAdd;
					latestHeadElementBytes = elementBytes;
					db.put(columnFamilyHandle, writeOptions, elementBytes, DUMMY_BYTES);
					return true;
				case 1: //head is less, i.e. higher prio
					db.put(columnFamilyHandle, writeOptions, elementBytes, DUMMY_BYTES);
					return false;
				case 0:
					return false;
			}
		} catch (RocksDBException rex) {
			throw new FlinkRuntimeException("Exception when putting queue element into RocksDB.", rex);
		}
		throw new IllegalStateException("Comparison problem.");
	}

	@Override
	public boolean remove(@Nonnull T toRemove) {

		if (isEmpty()) {
			return false;
		}

		final byte[] elementBytes = serializeElement(toRemove);
		final int compareWithHeadResult = LEXICOGRAPHICAL_COMPARATOR.compare(elementBytes, latestHeadElementBytes);
		try {
			switch (Integer.signum(compareWithHeadResult)) {
				case -1:
					// nothing should be smaller than head
					return false;
				case 1:
					db.delete(columnFamilyHandle, writeOptions, elementBytes);
					return false;
				case 0:
					db.delete(columnFamilyHandle, writeOptions, elementBytes);
					updateHead();
					return true;
			}
		} catch (RocksDBException rex) {
			throw new FlinkRuntimeException("Exception when putting queue element into RocksDB.", rex);
		}
		throw new IllegalStateException("Comparison problem.");
	}

	@Override
	public boolean isEmpty() {
		return headElement == null;
	}

	@Override
	public int size() {

		int count = 0;
		if (!isEmpty()) {
			try (final RocksIteratorWrapper iterator = internalIterator()) {
				while (iterator.isValid() && isPrefixWith(iterator.key(), groupPrefixBytes)) {
					++count;
					iterator.next();
				}
			}
		}
		return count;
	}

	@Override
	public void addAll(@Nullable Collection<? extends T> toAdd) {

		if (toAdd == null) {
			return;
		}

		for (T element : toAdd) {
			add(element);
		}
	}

	private RocksIteratorWrapper internalIterator() {
		final RocksIteratorWrapper iterator = new RocksIteratorWrapper(db.newIterator(columnFamilyHandle));
		// TODO we could check if it is more efficient to make the seek more specific, e.g. with a provided hint
		// that is lexicographically closer the first expected element in the key-group. I wonder if this could
		// help to improve the seek if there are many tombstones for elements at the beginning of the key-group
		// (like for elements that have been removed in previous polling, before they are compacted away).
		boolean failed = true;
		try {
			iterator.seek(latestHeadElementBytes);
			failed = false;
		} finally {
			if (failed) {
				iterator.close();
			}
		}
		return iterator;
	}

	@Nonnull
	@Override
	public RocksToJavaIteratorAdapter iterator() {
		return new RocksToJavaIteratorAdapter(internalIterator());
	}

	@Override
	public int getInternalIndex() {
		return internalIndex;
	}

	@Override
	public void setInternalIndex(int newIndex) {
		this.internalIndex = newIndex;
	}

	/**
	 * Adapter between RocksDB iterator and Java iterator. This is also closeable to release the native resources after
	 * use.
	 */
	private class RocksToJavaIteratorAdapter implements CloseableIterator<T> {

		/** The RocksDb iterator to which we forward ops. */
		@Nonnull
		private final RocksIteratorWrapper iterator;

		/** Cache for the current element bytes of the iteration. */
		@Nullable
		private byte[] nextElementBytes;

		private RocksToJavaIteratorAdapter(@Nonnull RocksIteratorWrapper iterator) {
			this.iterator = iterator;
			try {
				nextElementBytes = nextElementIfAvailable();
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
			return nextElementBytes != null;
		}

		@Override
		public T next() {
			if (nextElementBytes == null) {
				throw new NoSuchElementException("Iterator has no more elements!");
			}
			final T returnElement = deserializeElement(nextElementBytes);
			iterator.next();
			nextElementBytes = nextElementIfAvailable();
			return returnElement;
		}

		@Nonnull
		public byte[] getNextElementBytes() {
			if (nextElementBytes == null) {
				throw new NoSuchElementException("Iterator has no more elements!");
			}
			return nextElementBytes;
		}

		private byte[] nextElementIfAvailable() {
			byte[] elementBytes;
			return iterator.isValid()
				&& isPrefixWith((elementBytes = iterator.key()), groupPrefixBytes) ? elementBytes : null;
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

	private byte[] serializeElement(T element) {
		try {
			outputStream.reset();
			outputView.write(groupPrefixBytes);
			byteOrderProducingSerializer.serialize(element, outputView);
			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing the element.", e);
		}
	}

	private T deserializeElement(byte[] bytes) {
		try {
			// TODO introduce a stream in which we can change the internal byte[] to avoid creating instances per call
			ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(bytes);
			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
			inputView.skipBytes(groupPrefixBytes.length);
			return byteOrderProducingSerializer.deserialize(inputView);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while deserializing the element.", e);
		}
	}

	private void updateHead() {
		try (final RocksToJavaIteratorAdapter iterator = iterator()) {
			if (iterator.hasNext()) {
				latestHeadElementBytes = iterator.getNextElementBytes();
				headElement = iterator.next();
			} else {
				headElement = null;
			}
		}
	}
}
