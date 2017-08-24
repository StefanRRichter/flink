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

package org.apache.flink.streaming.util;

import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateImageMetaData;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.image.HeapFullKeyedStateImage;
import org.apache.flink.runtime.state.image.KeyedBackendStateImage;

import java.util.Collection;

public class StateHandleToStateImageConverter {

	private StateHandleToStateImageConverter() {
		throw new AssertionError();
	}

	@SuppressWarnings("unchecked")
	public static KeyedBackendStateImage convert(
		AbstractKeyedStateBackend<?> backend,
		Collection<KeyedStateHandle> stateHandles) {

		if (backend == null || stateHandles == null || stateHandles.isEmpty()) {
			return null;
		}

//		if (backend instanceof RocksDBKeyedStateImageRestore) {
//
//			KeyedStateHandle sampleStateHandle = stateHandles.iterator().next();
//
//			if(sampleStateHandle instanceof IncrementalKeyedStateHandle) {
//				return new RocksDBIncrementalKeyedStateImage(StateImageMetaData.PRIMARY_DFS, (Collection) stateHandles);
//			} else if (sampleStateHandle instanceof KeyGroupsStateHandle) {
//				return new RocksDBFullKeyedStateImage(StateImageMetaData.PRIMARY_DFS, (Collection) stateHandles);
//			} else {
//				throw new IllegalArgumentException("Unexpected state handle type: "+sampleStateHandle);
//			}
//
//		} else
		if (backend instanceof HeapKeyedStateBackend) {
			return new HeapFullKeyedStateImage(StateImageMetaData.PRIMARY_DFS, stateHandles);
		} else {
			throw new IllegalArgumentException("Unexpected backend type: " + backend);
		}

	}
}
