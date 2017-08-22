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

package org.apache.flink.runtime.state.image;

import org.apache.flink.runtime.state.StateImageMetaData;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.image.backends.HeapKeyedBackendImageRestore;
import org.apache.flink.runtime.state.image.backends.RocksDBKeyedStateImageRestore;
import org.apache.flink.util.Preconditions;

/**
 * TODO introduce RestoreException ?
 */
public abstract class KeyedBackendStateImage implements StateObject {

	private static final long serialVersionUID = 1L;

	protected final StateImageMetaData stateImageMetaData;

	protected KeyedBackendStateImage(StateImageMetaData stateImageMetaData) {
		this.stateImageMetaData = Preconditions.checkNotNull(stateImageMetaData);
	}

	public void restoreHeap(HeapKeyedBackendImageRestore heapKeyedStateBackend) throws Exception {
		throwUnsupportedOperationException(heapKeyedStateBackend);
	}

	public void restoreRocksDB(RocksDBKeyedStateImageRestore rocksDBKeyedStateBackend) throws Exception {
		throwUnsupportedOperationException(rocksDBKeyedStateBackend);
	}

	public StateImageMetaData getStateImageMetaData() {
		return stateImageMetaData;
	}

	private void throwUnsupportedOperationException(Object o) {
		Preconditions.checkNotNull(o);
		throw new UnsupportedOperationException(
			"This state image does not support restoring to an instance of " + o.getClass().getSimpleName() + ".");
	}
}
