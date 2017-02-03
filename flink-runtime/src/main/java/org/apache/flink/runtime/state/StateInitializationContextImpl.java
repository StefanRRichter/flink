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

package org.apache.flink.runtime.state;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistryClientView;
import org.apache.flink.core.fs.OwnedCloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Default implementation of {@link StateInitializationContext}.
 */
public class StateInitializationContextImpl implements StateInitializationContext, Closeable {

	/** Closable registry to participate in the operator's cancel/close methods */
	private final OwnedCloseableRegistry closableRegistry;

	/** Signal whether any state to restore was found */
	private final boolean restored;

	private final OperatorStateStore operatorStateStore;
	private final Collection<OperatorStateHandle> operatorStateHandles;

	private final KeyedStateStore keyedStateStore;
	private final Collection<KeyGroupsStateHandle> keyGroupsStateHandles;

	private final Iterable<KeyGroupStatePartitionStreamProvider> keyedStateIterable;

	public StateInitializationContextImpl(
			boolean restored,
			OperatorStateStore operatorStateStore,
			KeyedStateStore keyedStateStore,
			Collection<KeyGroupsStateHandle> keyGroupsStateHandles,
			Collection<OperatorStateHandle> operatorStateHandles) {

		this.restored = restored;
		this.closableRegistry = new OwnedCloseableRegistry();
		this.operatorStateStore = operatorStateStore;
		this.keyedStateStore = keyedStateStore;
		this.operatorStateHandles = operatorStateHandles;
		this.keyGroupsStateHandles = keyGroupsStateHandles;

		this.keyedStateIterable = keyGroupsStateHandles == null ?
				null
				: new Iterable<KeyGroupStatePartitionStreamProvider>() {
			@Override
			public Iterator<KeyGroupStatePartitionStreamProvider> iterator() {
				return new KeyGroupStreamIterator(getKeyGroupsStateHandles().iterator(), getClosableRegistry());
			}
		};
	}

	@Override
	public boolean isRestored() {
		return restored;
	}

	public Collection<OperatorStateHandle> getOperatorStateHandles() {
		return operatorStateHandles;
	}

	public Collection<KeyGroupsStateHandle> getKeyGroupsStateHandles() {
		return keyGroupsStateHandles;
	}

	public CloseableRegistryClientView getClosableRegistry() {
		return closableRegistry;
	}

	@Override
	public Iterable<StatePartitionStreamProvider> getRawOperatorStateInputs() {
		if (null != operatorStateHandles) {
			return new Iterable<StatePartitionStreamProvider>() {
				@Override
				public Iterator<StatePartitionStreamProvider> iterator() {
					return new OperatorStateStreamIterator(
							DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME,
							getOperatorStateHandles().iterator(), getClosableRegistry());
				}
			};
		} else {
			return Collections.emptyList();
		}
	}

	@Override
	public Iterable<KeyGroupStatePartitionStreamProvider> getRawKeyedStateInputs() {
		if(null == keyedStateStore) {
			throw new IllegalStateException("Attempt to access keyed state from non-keyed operator.");
		}

		if (null != keyGroupsStateHandles) {
			return keyedStateIterable;
		} else {
			return Collections.emptyList();
		}
	}

	@Override
	public OperatorStateStore getOperatorStateStore() {
		return operatorStateStore;
	}

	@Override
	public KeyedStateStore getKeyedStateStore() {
		return keyedStateStore;
	}

	@Override
	public void close() {
		IOUtils.closeQuietly(closableRegistry);
	}

	private static class KeyGroupStreamIterator
			extends AbstractStateStreamIterator<KeyGroupStatePartitionStreamProvider, KeyGroupsStateHandle> {

		private Iterator<Tuple2<Integer, Long>> currentOffsetsIterator;

		public KeyGroupStreamIterator(
				Iterator<KeyGroupsStateHandle> stateHandleIterator,
				CloseableRegistryClientView closableRegistry) {

			super(stateHandleIterator, closableRegistry);
		}

		@Override
		public boolean hasNext() {

			if (null != currentStateHandle && currentOffsetsIterator.hasNext()) {

				return true;
			}

			closeCurrentStream();

			while (stateHandleIterator.hasNext()) {
				currentStateHandle = stateHandleIterator.next();
				if (currentStateHandle.getNumberOfKeyGroups() > 0) {
					currentOffsetsIterator = currentStateHandle.getGroupRangeOffsets().iterator();

					return true;
				}
			}

			return false;
		}

		@Override
		public KeyGroupStatePartitionStreamProvider next() {

			if (!hasNext()) {

				throw new NoSuchElementException("Iterator exhausted");
			}

			Tuple2<Integer, Long> keyGroupOffset = currentOffsetsIterator.next();
			try {
				if (null == currentStream) {
					openCurrentStream();
				}
				currentStream.seek(keyGroupOffset.f1);

				return new KeyGroupStatePartitionStreamProvider(currentStream, keyGroupOffset.f0);
			} catch (IOException ioex) {

				return new KeyGroupStatePartitionStreamProvider(ioex, keyGroupOffset.f0);
			}
		}
	}

	private static class OperatorStateStreamIterator
			extends AbstractStateStreamIterator<StatePartitionStreamProvider, OperatorStateHandle> {

		private final String stateName; //TODO since we only support a single named state in raw, this could be dropped
		private long[] offsets;
		private int offPos;

		public OperatorStateStreamIterator(
				String stateName,
				Iterator<OperatorStateHandle> stateHandleIterator,
				CloseableRegistryClientView closableRegistry) {

			super(stateHandleIterator, closableRegistry);
			this.stateName = Preconditions.checkNotNull(stateName);
		}

		@Override
		public boolean hasNext() {

			if (null != offsets && offPos < offsets.length) {

				return true;
			}

			closeCurrentStream();

			while (stateHandleIterator.hasNext()) {
				currentStateHandle = stateHandleIterator.next();
				OperatorStateHandle.StateMetaInfo metaInfo =
						currentStateHandle.getStateNameToPartitionOffsets().get(stateName);

				if (null != metaInfo) {
					long[] metaOffsets = metaInfo.getOffsets();
					if (null != metaOffsets && metaOffsets.length > 0) {
						this.offsets = metaOffsets;
						this.offPos = 0;

						closableRegistry.unregister(currentStream);
						IOUtils.closeQuietly(currentStream);
						currentStream = null;

						return true;
					}
				}
			}

			return false;
		}

		@Override
		public StatePartitionStreamProvider next() {

			if (!hasNext()) {

				throw new NoSuchElementException("Iterator exhausted");
			}

			long offset = offsets[offPos++];

			try {
				if (null == currentStream) {
					openCurrentStream();
				}
				currentStream.seek(offset);

				return new StatePartitionStreamProvider(currentStream);
			} catch (IOException ioex) {

				return new StatePartitionStreamProvider(ioex);
			}
		}
	}

	abstract static class AbstractStateStreamIterator<T extends StatePartitionStreamProvider, H extends StreamStateHandle>
			implements Iterator<T> {

		protected final Iterator<H> stateHandleIterator;
		protected final CloseableRegistryClientView closableRegistry;

		protected H currentStateHandle;
		protected FSDataInputStream currentStream;

		public AbstractStateStreamIterator(
				Iterator<H> stateHandleIterator,
				CloseableRegistryClientView closableRegistry) {

			this.stateHandleIterator = Preconditions.checkNotNull(stateHandleIterator);
			this.closableRegistry = Preconditions.checkNotNull(closableRegistry);
		}

		protected void openCurrentStream() throws IOException {
			FSDataInputStream stream = currentStateHandle.openInputStream();
			closableRegistry.register(stream);
			currentStream = stream;
		}

		protected void closeCurrentStream() {
			closableRegistry.unregister(currentStream);
			IOUtils.closeQuietly(currentStream);
			currentStream = null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Read only Iterator");
		}
	}
}
