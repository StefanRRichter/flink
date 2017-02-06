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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;
import org.apache.flink.util.Preconditions;

public class StateTable<K, N, ST> {

	/** Map for holding the actual state objects. */
	private final VersionedHashMap<K, N, ST> state;

	/** Key group range which goes to this backend */
	private final KeyGroupRange keyGroupRange;

	/** Combined meta information such as name and serializers for this state */
	private RegisteredBackendStateMetaInfo<N, ST> metaInfo;

	// ------------------------------------------------------------------------
	public StateTable(
			VersionedHashMap<K, N, ST> state,
			RegisteredBackendStateMetaInfo<N, ST> metaInfo,
			KeyGroupRange keyGroupRange) {

		this.metaInfo = metaInfo;
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
		this.state = Preconditions.checkNotNull(state);
	}

	public StateTable(RegisteredBackendStateMetaInfo<N, ST> metaInfo, KeyGroupRange keyGroupRange) {
		this(new VersionedHashMap<K, N, ST>(), metaInfo, keyGroupRange);
	}

	// ------------------------------------------------------------------------
	//  access to maps
	// ------------------------------------------------------------------------

	public VersionedHashMap<K, N, ST> getState() {
		return state;
	}

	// ------------------------------------------------------------------------
	//  metadata
	// ------------------------------------------------------------------------
	
	public TypeSerializer<ST> getStateSerializer() {
		return metaInfo.getStateSerializer();
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return metaInfo.getNamespaceSerializer();
	}

	public RegisteredBackendStateMetaInfo<N, ST> getMetaInfo() {
		return metaInfo;
	}

	public void setMetaInfo(RegisteredBackendStateMetaInfo<N, ST> metaInfo) {
		this.metaInfo = metaInfo;
	}

	// ------------------------------------------------------------------------
	//  for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	boolean isEmpty() {
		return state.isEmpty();
	}
}
