/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * {@link ListState} implementation that stores state in RocksDB.
 *
 * <p>{@link RocksDBStateBackend} must ensure that we set the
 * {@link org.rocksdb.StringAppendOperator} on the column family that we use for our state since
 * we use the {@code merge()} call.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the values in the list state.
 */
public class RocksDBListState<K, N, V>
		extends AbstractRocksDBState<K, N, List<V>>
		implements InternalListState<K, N, V> {

	/** Serializer for the values. */
	private final TypeSerializer<V> elementSerializer;

	/**
	 * Separator of StringAppendTestOperator in RocksDB.
	 */
	private static final byte DELIMITER = ',';

	/**
	 * Creates a new {@code RocksDBListState}.
	 *
	 * @param columnFamily The RocksDB column family that this state is associated to.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer The serializer for the state.
	 * @param defaultValue The default value for the state.
	 * @param elementSerializer The serializer for elements of the list state.
	 * @param backend The backend for which this state is bind to.
	 */
	public RocksDBListState(
			ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<List<V>> valueSerializer,
			List<V> defaultValue,
			TypeSerializer<V> elementSerializer,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
		this.elementSerializer = elementSerializer;
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return backend.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<List<V>> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public Iterable<V> get() {
		try {

			byte[] key = serializeCurrentKeyWithGroupAndNamespace();
			byte[] valueBytes = backend.db.get(columnFamily, key);

			if (valueBytes == null) {
				return null;
			}

			ByteArrayInputStreamWithPos bais = new ByteArrayInputStreamWithPos(valueBytes);
			DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);

			List<V> result = new ArrayList<>();
			while (in.available() > 0) {
				result.add(elementSerializer.deserialize(in));
				if (in.available() > 0) {
					in.readByte();
				}
			}
			return result;
		} catch (IOException | RocksDBException e) {
			throw new RuntimeException("Error while retrieving data from RocksDB", e);
		}
	}

	@Override
	public void add(V value) {
		Preconditions.checkNotNull(value, "You cannot add null to a ListState.");

		try {
			backend.db.merge(
				columnFamily,
				writeOptions,
				serializeCurrentKeyWithGroupAndNamespace(),
				serializeValue(value, elementSerializer));
		} catch (Exception e) {
			throw new RuntimeException("Error while adding data to RocksDB", e);
		}
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		if (sources == null || sources.isEmpty()) {
			return;
		}

		try {
			// create the target full-binary-key
			setCurrentNamespace(target);
			final byte[] targetKey = serializeCurrentKeyWithGroupAndNamespace();

			// merge the sources to the target
			for (N source : sources) {
				if (source != null) {

					setCurrentNamespace(source);
					final byte[] sourceKey = serializeCurrentKeyWithGroupAndNamespace();

					byte[] valueBytes = backend.db.get(columnFamily, sourceKey);
					backend.db.delete(columnFamily, writeOptions, sourceKey);

					if (valueBytes != null) {
						backend.db.merge(columnFamily, writeOptions, targetKey, valueBytes);
					}
				}
			}
		}
		catch (Exception e) {
			throw new Exception("Error while merging state in RocksDB", e);
		}
	}

	@Override
	public void update(List<V> values) {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");

		clear();

		if (!values.isEmpty()) {
			try {
				backend.db.put(
					columnFamily,
					writeOptions,
					serializeCurrentKeyWithGroupAndNamespace(),
					serializeValueList(values, elementSerializer, DELIMITER));
			} catch (IOException | RocksDBException e) {
				throw new RuntimeException("Error while updating data to RocksDB", e);
			}
		}
	}

	@Override
	public void addAll(List<V> values) {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");

		if (!values.isEmpty()) {
			try {
				backend.db.merge(
					columnFamily,
					writeOptions,
					serializeCurrentKeyWithGroupAndNamespace(),
					serializeValueList(values, elementSerializer, DELIMITER));
			} catch (IOException | RocksDBException e) {
				throw new RuntimeException("Error while updating data to RocksDB", e);
			}
		}
	}
}
