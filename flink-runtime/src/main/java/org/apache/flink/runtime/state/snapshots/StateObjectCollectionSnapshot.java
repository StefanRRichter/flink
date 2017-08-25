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

package org.apache.flink.runtime.state.snapshots;

import org.apache.flink.runtime.state.SnapshotMetaData;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;

public abstract class StateObjectCollectionSnapshot<T extends StateObject> extends Snapshot {

	private static final long serialVersionUID = 1L;

	protected final Collection<T> stateObjects;

	protected StateObjectCollectionSnapshot(SnapshotMetaData metaData, Collection<T> stateObjects) {
		super(metaData);
		this.stateObjects = Collections.unmodifiableCollection(Preconditions.checkNotNull(stateObjects));
		Preconditions.checkState(!stateObjects.isEmpty());
	}

	public Collection<T> getStateObjects() {
		return stateObjects;
	}

	@Override
	public void discardState() throws Exception {
		StateUtil.bestEffortDiscardAllStateObjects(stateObjects);
	}

	@Override
	public long getStateSize() {
		long size = 0L;

		for (StateObject stateHandle : stateObjects) {
			size += stateHandle.getStateSize();
		}

		return size;
	}
}
