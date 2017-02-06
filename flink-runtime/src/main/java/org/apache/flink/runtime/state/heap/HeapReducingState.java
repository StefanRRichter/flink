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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Heap-backed partitioned {@link org.apache.flink.api.common.state.ReducingState} that is
 * snapshotted into files.
 * 
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class HeapReducingState<K, N, V>
		extends AbstractHeapMergingState<K, N, V, V, V, ReducingState<V>, ReducingStateDescriptor<V>>
		implements InternalReducingState<N, V> {

	private final ReduceFunction<V> reduceFunction;

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param backend The state backend backing that created this state.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param stateTable The state table to use in this kev/value state. May contain initial state.
	 */
	public HeapReducingState(
			KeyedStateBackend<K> backend,
			ReducingStateDescriptor<V> stateDesc,
			StateTable<K, N, V> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {

		super(backend, stateDesc, stateTable, keySerializer, namespaceSerializer);
		this.reduceFunction = stateDesc.getReduceFunction();
	}

	// ------------------------------------------------------------------------
	//  state access
	// ------------------------------------------------------------------------

	@Override
	public V get() {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		VersionedHashMap<K, N, V>  keyNamespaceVMap = stateTable.getState();
		return keyNamespaceVMap.get(backend.getCurrentKey(), currentNamespace);
	}

	@Override
	public void add(V value) throws IOException {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		if (value == null) {
			clear();
			return;
		}

		final VersionedHashMap<K, N, V> keyNamespaceVMap = stateTable.getState();

		final K key = backend.getCurrentKey();
		final N namespace = currentNamespace;
		final V currentValue = keyNamespaceVMap.put(key, namespace, value);

		if (currentValue != null) {
			V reducedValue;
			try {
				reducedValue = reduceFunction.reduce(currentValue, value);
			} catch (Exception e) {
				throw new IOException("Exception while applying ReduceFunction in reducing state", e);
			}
			keyNamespaceVMap.put(key, namespace, reducedValue);
		}
	}

	// ------------------------------------------------------------------------
	//  state merging
	// ------------------------------------------------------------------------

	@Override
	protected V mergeState(V a, V b) throws Exception {
		return reduceFunction.reduce(a, b);
	}
}
