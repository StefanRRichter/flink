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

package org.apache.flink.runtime.state.snapshot;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class KeyedStateHandleSnapshot
	extends AbstractHandleBasedSnapshot<KeyedStateHandle>
	implements KeyedStateSnapshot {

	public KeyedStateHandleSnapshot(
		@Nonnull SnapshotMetaData metaData,
		@Nonnull Collection<KeyedStateHandle> stateObjects) {

		super(metaData, stateObjects);
	}

	public KeyedStateHandleSnapshot(
		@Nonnull SnapshotMetaData metaData,
		@Nonnull KeyedStateHandle stateObjects) {
		this(metaData, Collections.singletonList(stateObjects));
	}

//	@Override
//	public KeyedStateSnapshot getSnapshotForIntersection(KeyGroupRange keyGroupRange) {
//		return new KeyedStateHandleSnapshot(metaData, getKeyedStateHandles(stateObjects, keyGroupRange));
//	}
//
//	@Override
//	public void registerSharedState(SharedStateRegistry registry) {
//		for (KeyedStateHandle stateObject : stateObjects) {
//			stateObject.registerSharedStates(registry);
//		}
//	}
//
//	private static List<KeyedStateHandle> getKeyedStateHandles(
//		Iterable<? extends KeyedStateHandle> keyedStateHandles,
//		KeyGroupRange subtaskKeyGroupRange) {
//
//		List<KeyedStateHandle> subtaskKeyedStateHandles = new ArrayList<>();
//
//		for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {
//			KeyedStateHandle intersectedKeyedStateHandle = keyedStateHandle.getIntersection(subtaskKeyGroupRange);
//
//			if (intersectedKeyedStateHandle != null) {
//				subtaskKeyedStateHandles.add(intersectedKeyedStateHandle);
//			}
//		}
//
//		return subtaskKeyedStateHandles;
//	}
}
