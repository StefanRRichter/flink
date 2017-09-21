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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.util.OperatorSubtaskDescriptionText;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created in setup or initialize state phase of an operator.
 * <p>
 * TODO raw streams should only be created once and handed out as always the same instance.
 */
public class StreamOperatorStateManager {

	private static final Logger LOG = LoggerFactory.getLogger(StreamOperatorStateManager.class);

//	private TaskStateSnapshot jobManagerState; // TODO get from task state manager

	private final OperatorSubtaskState operatorSubtaskStateFromJobManager;

	private final ProcessingTimeService processingTimeService;

	private final TaskStateManager taskStateManager;

	private final StateBackend stateBackend;

	private final StreamOperator<?> operator;

	final StreamConfig operatorConfig;

	private final CloseableRegistry closeableRegistry; // from "container" aka StreamTask

	private final Environment env;

	private final String operatorIdentifierText;

	public StreamOperatorStateManager(
		StreamOperator<?> operator,
		StreamConfig operatorConfig,
		TaskStateManager taskStateManager,
		StateBackend stateBackend,
		ProcessingTimeService processingTimeService,
		CloseableRegistry closeableRegistry,
		Environment env) {

		this.operator = Preconditions.checkNotNull(operator);
		this.operatorConfig = Preconditions.checkNotNull(operatorConfig);
		this.taskStateManager = Preconditions.checkNotNull(taskStateManager);
		this.stateBackend = Preconditions.checkNotNull(stateBackend);
		this.processingTimeService = Preconditions.checkNotNull(processingTimeService);
		this.closeableRegistry = Preconditions.checkNotNull(closeableRegistry);
		this.env = Preconditions.checkNotNull(env);

		this.operatorSubtaskStateFromJobManager = taskStateManager.operatorStates(operator.getOperatorID());

		TaskInfo taskInfo = env.getTaskInfo();

		OperatorSubtaskDescriptionText operatorSubtaskDescription =
			new OperatorSubtaskDescriptionText(
				operator.getOperatorID(),
				operator.getClass(),
				taskInfo.getIndexOfThisSubtask(),
				taskInfo.getNumberOfParallelSubtasks());

		this.operatorIdentifierText = operatorSubtaskDescription.toString();
	}

	/**
	 * This is the main (and only ?) publicly called method
	 */
	public StreamOperatorStateContext streamOperatorStateContext() throws Exception {

		final boolean restoring = (operatorSubtaskStateFromJobManager != null); //TODO!! change a bit: -> if operatorID is "known".

		final TaskInfo taskInfo = env.getTaskInfo();
		final ClassLoader userClassLoader = env.getUserClassLoader();
		final TypeSerializer<?> keySerializer = operatorConfig.getStateKeySerializer(userClassLoader);
		final KeyGroupRange keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
			taskInfo.getMaxNumberOfParallelSubtasks(),
			taskInfo.getNumberOfParallelSubtasks(),
			taskInfo.getIndexOfThisSubtask());

		// -------------- Keyed State Backend --------------
		final AbstractKeyedStateBackend<?> keyedStatedBackend = createKeyedStatedBackend(keySerializer, keyGroupRange);

		// -------------- Operator State Backend --------------
		final OperatorStateBackend operatorStateBackend = createOperatorStateBackend();

		// -------------- Raw State Streams --------------
		final Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs = getRawKeyedStateInputs();
		final Iterable<StatePartitionStreamProvider> rawOperatorStateInputs = getRawOperatorStateInputs();

		// -------------- Internal Timer Service Manager --------------
		final InternalTimeServiceManager<?, ?> timeServiceManager =
			createInternalTimeServiceManager(keyedStatedBackend, rawKeyedStateInputs);

		// -------------- Checkpoint Stream Factory --------------
		CheckpointStreamFactory checkpointStreamFactory =
			stateBackend.createStreamFactory(env.getJobID(), operatorIdentifierText);

		// -------------- Preparing return value --------------

		return new StreamOperatorStateContextImpl(
			restoring,
			operatorStateBackend,
			keyedStatedBackend,
			timeServiceManager,
			rawOperatorStateInputs,
			rawKeyedStateInputs,
			checkpointStreamFactory);
	}

	public <K> InternalTimeServiceManager<?, K> createInternalTimeServiceManager(
		AbstractKeyedStateBackend<K> keyedStatedBackend,
		Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStates) throws Exception {

		if (keyedStatedBackend == null) {
			return null;
		}

		final KeyGroupRange keyGroupRange = keyedStatedBackend.getKeyGroupRange();

		final InternalTimeServiceManager<?, K> timeServiceManager = new InternalTimeServiceManager<>(
			keyedStatedBackend.getNumberOfKeyGroups(),
			keyGroupRange,
			operator,
			processingTimeService);

		// and then initialize the timer services
		for (KeyGroupStatePartitionStreamProvider streamProvider : rawKeyedStates) {
			int keyGroupIdx = streamProvider.getKeyGroupId();

			Preconditions.checkArgument(keyGroupRange.contains(keyGroupIdx),
				"Key Group " + keyGroupIdx + " does not belong to the local range.");

			timeServiceManager.restoreStateForKeyGroup(
				new DataInputViewStreamWrapper(streamProvider.getStream()),
				keyGroupIdx, env.getUserClassLoader());
		}

		return timeServiceManager;
	}

	public <K> AbstractKeyedStateBackend<K> createKeyedStatedBackend(
		TypeSerializer<K> keySerializer,
		KeyGroupRange keyGroupRange) throws Exception {

		if (keySerializer == null) {
			return null;
		}

		//TODO search in local state for a local recovery opportunity.

		return createKeyedStatedBackendFromJobManagerState(keySerializer, keyGroupRange);
	}

	public OperatorStateBackend createOperatorStateBackend() throws Exception {

		//TODO search in local state for a local recovery opportunity.

		return createOperatorStateBackendFromJobManagerState();
	}

	public boolean isRestored() {
		throw new UnsupportedOperationException("TODO");
	}

	private OperatorStateBackend createOperatorStateBackendFromJobManagerState() throws Exception {

		OperatorStateBackend operatorStateBackend =
			stateBackend.createOperatorStateBackend(env, operatorIdentifierText);

		// let operator state backend participate in the operator lifecycle, i.e. make it responsive to cancelation
		closeableRegistry.registerCloseable(operatorStateBackend);

		Collection<OperatorStateHandle> managedOperatorState = null;

		if (operatorSubtaskStateFromJobManager != null) {
			managedOperatorState = operatorSubtaskStateFromJobManager.getManagedOperatorState();
		}

		operatorStateBackend.restore(managedOperatorState);

		return operatorStateBackend;
	}

	private <K> AbstractKeyedStateBackend<K> createKeyedStatedBackendFromJobManagerState(
		TypeSerializer<K> keySerializer,
		KeyGroupRange keyGroupRange) throws Exception {

		AbstractKeyedStateBackend<K> keyedStateBackend = stateBackend.createKeyedStateBackend(
			env,
			env.getJobID(),
			operatorIdentifierText,
			keySerializer,
			env.getTaskInfo().getMaxNumberOfParallelSubtasks(), //TODO check: this is numberOfKeyGroups!!!!
			keyGroupRange,
			env.getTaskKvStateRegistry());

		closeableRegistry.registerCloseable(keyedStateBackend);

		Collection<KeyedStateHandle> managedKeyedState = null;

		if (operatorSubtaskStateFromJobManager != null) {
			managedKeyedState = operatorSubtaskStateFromJobManager.getManagedKeyedState();
		}

		keyedStateBackend.restore(managedKeyedState);

		return keyedStateBackend;
	}

	public Iterable<StatePartitionStreamProvider> getRawOperatorStateInputs() {

		if (operatorSubtaskStateFromJobManager != null) {

			Collection<OperatorStateHandle> rawOperatorState =
				operatorSubtaskStateFromJobManager.getRawOperatorState();

			return () -> new OperatorStateStreamIterator(
				DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME,
				rawOperatorState.iterator(), closeableRegistry);
		}

		return Collections.emptyList();
	}

	public Iterable<KeyGroupStatePartitionStreamProvider> getRawKeyedStateInputs() {

		if (operatorSubtaskStateFromJobManager != null) {

			Collection<KeyedStateHandle> rawKeyedState = operatorSubtaskStateFromJobManager.getRawKeyedState();
			Collection<KeyGroupsStateHandle> keyGroupsStateHandles = transform(rawKeyedState);

			return () -> new KeyGroupStreamIterator(keyGroupsStateHandles.iterator(), closeableRegistry);
		}

		return Collections.emptyList();
	}

	// =================================================================================================================

	private static class KeyGroupStreamIterator
		extends AbstractStateStreamIterator<KeyGroupStatePartitionStreamProvider, KeyGroupsStateHandle> {

		private Iterator<Tuple2<Integer, Long>> currentOffsetsIterator;

		public KeyGroupStreamIterator(
			Iterator<KeyGroupsStateHandle> stateHandleIterator, CloseableRegistry closableRegistry) {

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
				if (currentStateHandle.getKeyGroupRange().getNumberOfKeyGroups() > 0) {
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
			CloseableRegistry closableRegistry) {

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

						if (closableRegistry.unregisterCloseable(currentStream)) {
							IOUtils.closeQuietly(currentStream);
							currentStream = null;
						}

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

	private abstract static class AbstractStateStreamIterator<T extends StatePartitionStreamProvider, H extends StreamStateHandle>
		implements Iterator<T> {

		protected final Iterator<H> stateHandleIterator;
		protected final CloseableRegistry closableRegistry;

		protected H currentStateHandle;
		protected FSDataInputStream currentStream;

		public AbstractStateStreamIterator(
			Iterator<H> stateHandleIterator,
			CloseableRegistry closableRegistry) {

			this.stateHandleIterator = Preconditions.checkNotNull(stateHandleIterator);
			this.closableRegistry = Preconditions.checkNotNull(closableRegistry);
		}

		protected void openCurrentStream() throws IOException {

			Preconditions.checkState(currentStream == null);

			FSDataInputStream stream = currentStateHandle.openInputStream();
			closableRegistry.registerCloseable(stream);
			currentStream = stream;
		}

		protected void closeCurrentStream() {
			if (closableRegistry.unregisterCloseable(currentStream)) {
				IOUtils.closeQuietly(currentStream);
			}
			currentStream = null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Read only Iterator");
		}
	}

	private static Collection<KeyGroupsStateHandle> transform(Collection<KeyedStateHandle> keyedStateHandles) {

		if (keyedStateHandles == null) {
			return null;
		}

		List<KeyGroupsStateHandle> keyGroupsStateHandles = new ArrayList<>(keyedStateHandles.size());

		for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {

			if (keyedStateHandle instanceof KeyGroupsStateHandle) {
				keyGroupsStateHandles.add((KeyGroupsStateHandle) keyedStateHandle);
			} else if (keyedStateHandle != null) {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected: " + KeyGroupsStateHandle.class +
					", but found: " + keyedStateHandle.getClass() + ".");
			}
		}

		return keyGroupsStateHandles;
	}

	private static class StreamOperatorStateContextImpl implements StreamOperatorStateContext {

		private final boolean restored;

		private final OperatorStateBackend operatorStateBackend;
		private final AbstractKeyedStateBackend<?> keyedStateBackend;
		private final InternalTimeServiceManager<?, ?> internalTimeServiceManager;

		private final Iterable<StatePartitionStreamProvider> rawOperatorStateInputs;
		private final Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs;

		private final CheckpointStreamFactory checkpointStreamFactory;

		public StreamOperatorStateContextImpl(
			boolean restored,
			OperatorStateBackend operatorStateBackend,
			AbstractKeyedStateBackend<?> keyedStateBackend,
			InternalTimeServiceManager<?, ?> internalTimeServiceManager,
			Iterable<StatePartitionStreamProvider> rawOperatorStateInputs,
			Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs,
			CheckpointStreamFactory checkpointStreamFactory) {

			this.restored = restored;
			this.operatorStateBackend = operatorStateBackend;
			this.keyedStateBackend = keyedStateBackend;
			this.internalTimeServiceManager = internalTimeServiceManager;
			this.rawOperatorStateInputs = rawOperatorStateInputs;
			this.rawKeyedStateInputs = rawKeyedStateInputs;
			this.checkpointStreamFactory = checkpointStreamFactory;
		}

		@Override
		public boolean isRestored() {
			return restored;
		}

		@Override
		public AbstractKeyedStateBackend<?> keyedStateBackend() throws Exception {
			return keyedStateBackend;
		}

		@Override
		public OperatorStateBackend operatorStateBackend() throws Exception {
			return operatorStateBackend;
		}

		@Override
		public InternalTimeServiceManager<?, ?> internalTimerServiceManager() throws Exception {
			return internalTimeServiceManager;
		}

		@Override
		public CheckpointStreamFactory checkpointStreamFactory() throws IOException {
			return checkpointStreamFactory;
		}

		@Override
		public Iterable<StatePartitionStreamProvider> rawOperatorStateInputs() {
			return rawOperatorStateInputs;
		}

		@Override
		public Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs() {
			return rawKeyedStateInputs;
		}
	}
}
