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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.io.async.AbstractAsyncIOCallable;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Abstract base class for async IO operations of snapshots against a
 * {@link java.util.zip.CheckedOutputStream}. This includes participating in lifecycle management
 * through a {@link CloseableRegistry}.
 */
public abstract class AbstractAsyncSnapshotIOCallable<H extends StateObject>
	extends AbstractAsyncIOCallable<H, CheckpointStreamFactory.CheckpointStateOutputStream> {

	protected final  long checkpointId;
	protected final  long timestamp;

	protected final CheckpointStreamFactory streamFactory;
	protected final CloseableRegistry closeStreamOnCancelRegistry;

	public AbstractAsyncSnapshotIOCallable(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory streamFactory,
		CloseableRegistry closeStreamOnCancelRegistry) {

		this.streamFactory = Preconditions.checkNotNull(streamFactory);
		this.closeStreamOnCancelRegistry = Preconditions.checkNotNull(closeStreamOnCancelRegistry);
		this.checkpointId = checkpointId;
		this.timestamp = timestamp;
	}

	@Override
	public CheckpointStreamFactory.CheckpointStateOutputStream openIOHandle() throws Exception {
		CheckpointStreamFactory.CheckpointStateOutputStream stream =
			streamFactory.createCheckpointStateOutputStream(checkpointId, timestamp);
		closeStreamOnCancelRegistry.registerClosable(stream);
		return stream;
	}

	protected synchronized StreamStateHandle closeStreamAndGetStateHandle() throws IOException {
		final CheckpointStreamFactory.CheckpointStateOutputStream stream = getIoHandle();
		if (stream != null) {
			return stream.closeAndGetHandle();
		} else {
			throw new IOException("No active stream to a obtain handle.");
		}
	}

	@Override
	protected void doCloseInternal(CheckpointStreamFactory.CheckpointStateOutputStream stream)
		throws IOException {
		closeStreamOnCancelRegistry.unregisterClosable(stream);
		super.doCloseInternal(stream);
	}
}
