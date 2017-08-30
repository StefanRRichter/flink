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

import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 *
 * @param <T>
 */
public abstract class Snapshot<T extends StateObject> implements StateObject, Iterable<T> {

	private static final long serialVersionUID = 1L;

	/** */
	@Nonnull protected final SnapshotMetaData metaData;

	/** */
	@Nonnull protected final Collection<T> stateObjects;

	protected Snapshot(@Nonnull SnapshotMetaData metaData, @Nonnull Collection<? extends T> stateObjects) {
		this.metaData = Preconditions.checkNotNull(metaData);
		this.stateObjects = Collections.unmodifiableCollection(Preconditions.checkNotNull(stateObjects));
	}

	@Nonnull
	public SnapshotMetaData getMetaData() {
		return metaData;
	}

	@Nonnull
	@Override
	public Iterator<T> iterator() {
		return stateObjects.iterator();
	}

	@Override
	public void discardState() throws Exception {
		StateUtil.bestEffortDiscardAllStateObjects(stateObjects);
	}

	@Override
	public long getStateSize() {
		long stateSize = 0L;
		for (T stateObject : stateObjects) {
			stateSize += stateObject.getStateSize();
		}
		return stateSize;
	}
}
