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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;

import java.io.IOException;

/**
 * Heap-backed partitioned {@link ReducingState} that is
 * snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <IN> The type of the value added to the state.
 * @param <ACC> The type of the value stored in the state (the accumulator type).
 * @param <OUT> The type of the value returned from the state.
 */
public class HeapAggregatingState<K, N, IN, ACC, OUT>
		extends AbstractHeapMergingState<K, N, IN, OUT, ACC, AggregatingState<IN, OUT>, AggregatingStateDescriptor<IN, ACC, OUT>>
		implements InternalAggregatingState<N, IN, OUT> {

	private final AggregateFunction<IN, ACC, OUT> aggFunction;

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateDesc
	 *             The state identifier for the state. This contains name and can create a default state value.
	 * @param stateTable
	 *             The state table to use in this kev/value state. May contain initial state.
	 * @param namespaceSerializer
	 *             The serializer for the type that indicates the namespace
	 */
	public HeapAggregatingState(
			AggregatingStateDescriptor<IN, ACC, OUT> stateDesc,
			StateTable<K, N, ACC> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {

		super(stateDesc, stateTable, keySerializer, namespaceSerializer);
		this.aggFunction = stateDesc.getAggregateFunction();
	}

	// ------------------------------------------------------------------------
	//  state access
	// ------------------------------------------------------------------------

	@Override
	public OUT get() {

		ACC accumulator = stateTable.get(currentNamespace);
		return accumulator != null ? aggFunction.getResult(accumulator) : null;
	}

	@Override
	public void add(IN value) throws IOException {
		final N namespace = currentNamespace;

		if (value == null) {
			clear();
			return;
		}

		final StateTable<K, N, ACC> map = stateTable;

		// if this is the first value for the key, create a new accumulator
		ACC accumulator = map.get(namespace);
		if (accumulator == null) {
			accumulator = aggFunction.createAccumulator();
			map.put(namespace, accumulator);
		}

		aggFunction.add(value, accumulator);
	}

	// ------------------------------------------------------------------------
	//  state merging
	// ------------------------------------------------------------------------

	@Override
	protected ACC mergeState(ACC a, ACC b) throws Exception {
		return aggFunction.merge(a, b);
	}
}