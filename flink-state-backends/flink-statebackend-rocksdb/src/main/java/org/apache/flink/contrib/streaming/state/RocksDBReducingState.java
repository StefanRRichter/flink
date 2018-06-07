/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.internal.InternalReducingState;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;

/**
 * {@link ReducingState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
public class RocksDBReducingState<K, N, V>
		extends AbstractRocksDBState<K, N, V>
		implements InternalReducingState<K, N, V> {

	/** User-specified reduce function. */
	private final ReduceFunction<V> reduceFunction;

	/**
	 * Creates a new {@code RocksDBReducingState}.
	 *
	 * @param columnFamily The RocksDB column family that this state is associated to.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer The serializer for the state.
	 * @param defaultValue The default value for the state.
	 * @param reduceFunction The reduce function used for reducing state.
	 * @param backend The backend for which this state is bind to.
	 */
	public RocksDBReducingState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> valueSerializer,
			V defaultValue,
			ReduceFunction<V> reduceFunction,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
		this.reduceFunction = reduceFunction;
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
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public V get() {
		try {
			byte[] key = serializeCurrentKeyWithGroupAndNamespace();
			byte[] valueBytes = backend.db.get(columnFamily, key);
			if (valueBytes == null) {
				return null;
			}
			return valueSerializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(valueBytes)));
		} catch (IOException | RocksDBException e) {
			throw new RuntimeException("Error while retrieving data from RocksDB", e);
		}
	}

	@Override
	public void add(V value) {
		try {
			byte[] key = serializeCurrentKeyWithGroupAndNamespace();
			byte[] valueBytes = backend.db.get(columnFamily, key);

			final V newValue = valueBytes == null ?
				value :
				reduceFunction.reduce(
					valueSerializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(valueBytes))),
					value);

			backend.db.put(columnFamily, writeOptions, key, serializeValue(newValue));
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
			V current = null;

			// merge the sources to the target
			for (N source : sources) {
				if (source != null) {

					setCurrentNamespace(source);
					final byte[] sourceKey = serializeCurrentKeyWithGroupAndNamespace();

					final byte[] valueBytes = backend.db.get(columnFamily, sourceKey);
					backend.db.delete(columnFamily, writeOptions, sourceKey);

					if (valueBytes != null) {
						V value = valueSerializer.deserialize(
								new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(valueBytes)));

						current = (current != null) ? reduceFunction.reduce(current, value) : value;
					}
				}
			}

			// if something came out of merging the sources, merge it or write it to the target
			if (current != null) {
				// create the target full-binary-key
				setCurrentNamespace(target);
				final byte[] targetKey = serializeCurrentKeyWithGroupAndNamespace();

				final byte[] targetValueBytes = backend.db.get(columnFamily, targetKey);

				if (targetValueBytes != null) {
					// target also had a value, merge
					V value = valueSerializer.deserialize(
							new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(targetValueBytes)));

					current = reduceFunction.reduce(current, value);
				}

				// write the resulting value
				backend.db.put(columnFamily, writeOptions, targetKey, serializeValue(current));
			}
		}
		catch (Exception e) {
			throw new Exception("Error while merging state in RocksDB", e);
		}
	}
}
