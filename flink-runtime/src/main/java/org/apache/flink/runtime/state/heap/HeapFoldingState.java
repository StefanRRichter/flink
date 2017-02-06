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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Heap-backed partitioned {@link FoldingState} that is
 * snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <T> The type of the values that can be folded into the state.
 * @param <ACC> The type of the value in the folding state.
 */
public class HeapFoldingState<K, N, T, ACC>
		extends AbstractHeapState<K, N, ACC, FoldingState<T, ACC>, FoldingStateDescriptor<T, ACC>>
		implements InternalFoldingState<N, T, ACC> {

	/** The function used to fold the state */
	private final FoldFunction<T, ACC> foldFunction;

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param backend The state backend backing that created this state.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param stateTable The state tab;e to use in this kev/value state. May contain initial state.
	 */
	public HeapFoldingState(
			KeyedStateBackend<K> backend,
			FoldingStateDescriptor<T, ACC> stateDesc,
			StateTable<K, N, ACC> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {
		super(backend, stateDesc, stateTable, keySerializer, namespaceSerializer);
		this.foldFunction = stateDesc.getFoldFunction();
	}

	// ------------------------------------------------------------------------
	//  state access
	// ------------------------------------------------------------------------

	@Override
	public ACC get() {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		VersionedHashMap<K, N, ACC> namespaceMap = stateTable.getState();
		return namespaceMap.get(backend.getCurrentKey(), namespaceMap);
	}

	@Override
	public void add(T value) throws IOException {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		if (value == null) {
			clear();
			return;
		}

		final VersionedHashMap<K, N, ACC> namespaceMap = stateTable.getState();
		final K key = backend.getCurrentKey();
		final N namespace = currentNamespace;
		final ACC currentValue = namespaceMap.get(key, namespace);

		try {

			if (currentValue == null) {
				namespaceMap.put(key, namespace,
						foldFunction.fold(stateDesc.getDefaultValue(), value));
			} else {
				namespaceMap.put(key, namespace, foldFunction.fold(currentValue, value));
			}
		} catch (Exception e) {
			throw new RuntimeException("Could not add value to folding state.", e);
		}
	}
}
