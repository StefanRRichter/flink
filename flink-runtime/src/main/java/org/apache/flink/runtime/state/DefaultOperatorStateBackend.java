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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.async.AbstractAsyncCallableWithResources;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.runtime.state.metainfo.StateMetaInfo;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoBase;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RunnableFuture;

/**
 * Default implementation of OperatorStateStore that provides the ability to make snapshots.
 */
@Internal
public class DefaultOperatorStateBackend implements OperatorStateBackend {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultOperatorStateBackend.class);

	/**
	 * The default namespace for state in cases where no state name is provided
	 */
	public static final String DEFAULT_OPERATOR_STATE_NAME = "_default_";

	/**
	 * Map for all registered operator states. Maps state name -> state
	 */
	private final Map<String, PartitionableListState<?>> registeredOperatorStates;

	/**
	 * Map for all registered operator broadcast states. Maps state name -> state
	 */
	private final Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStates;

	/**
	 * CloseableRegistry to participate in the tasks lifecycle.
	 */
	private final CloseableRegistry closeStreamOnCancelRegistry;

	/**
	 * Default serializer. Only used for the default operator state.
	 */
	private final JavaSerializer<Serializable> javaSerializer;

	/**
	 * The user code classloader.
	 */
	private final ClassLoader userClassloader;

	/**
	 * The execution configuration.
	 */
	private final ExecutionConfig executionConfig;

	/**
	 * Flag to de/activate asynchronous snapshots.
	 */
	private final boolean asynchronousSnapshots;

	/**
	 * Map of state names to their corresponding restored state meta info.
	 *
	 * <p>TODO this map can be removed when eager-state registration is in place.
	 * TODO we currently need this cached to check state migration strategies when new serializers are registered.
	 */
	private final Map<String, StateMetaInfo.Snapshot> restoredOperatorStateMetaInfos;

	/**
	 * Map of state names to their corresponding restored broadcast state meta info.
	 */
	private final Map<String, StateMetaInfo.Snapshot> restoredBroadcastStateMetaInfos;

	/**
	 * Cache of already accessed states.
	 *
	 * <p>In contrast to {@link #registeredOperatorStates} and {@link #restoredOperatorStateMetaInfos} which may be repopulated
	 * with restored state, this map is always empty at the beginning.
	 *
	 * <p>TODO this map should be moved to a base class once we have proper hierarchy for the operator state backends.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-6849">FLINK-6849</a>
	 */
	private final HashMap<String, PartitionableListState<?>> accessedStatesByName;

	private final Map<String, BackendWritableBroadcastState<?, ?>> accessedBroadcastStatesByName;

	public DefaultOperatorStateBackend(
		ClassLoader userClassLoader,
		ExecutionConfig executionConfig,
		boolean asynchronousSnapshots) {

		this.closeStreamOnCancelRegistry = new CloseableRegistry();
		this.userClassloader = Preconditions.checkNotNull(userClassLoader);
		this.executionConfig = executionConfig;
		this.javaSerializer = new JavaSerializer<>();
		this.registeredOperatorStates = new HashMap<>();
		this.registeredBroadcastStates = new HashMap<>();
		this.asynchronousSnapshots = asynchronousSnapshots;
		this.accessedStatesByName = new HashMap<>();
		this.accessedBroadcastStatesByName = new HashMap<>();
		this.restoredOperatorStateMetaInfos = new HashMap<>();
		this.restoredBroadcastStateMetaInfos = new HashMap<>();
	}

	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	@Override
	public Set<String> getRegisteredStateNames() {
		return registeredOperatorStates.keySet();
	}

	@Override
	public Set<String> getRegisteredBroadcastStateNames() {
		return registeredBroadcastStates.keySet();
	}

	@Override
	public void close() throws IOException {
		closeStreamOnCancelRegistry.close();
	}

	@Override
	public void dispose() {
		IOUtils.closeQuietly(closeStreamOnCancelRegistry);
		registeredOperatorStates.clear();
		registeredBroadcastStates.clear();
	}

	// -------------------------------------------------------------------------------------------
	//  State access methods
	// -------------------------------------------------------------------------------------------

	@Override
	public <K, V> BroadcastState<K, V> getBroadcastState(final MapStateDescriptor<K, V> stateDescriptor) throws StateMigrationException {

		Preconditions.checkNotNull(stateDescriptor);
		String name = Preconditions.checkNotNull(stateDescriptor.getName());

		@SuppressWarnings("unchecked")
		BackendWritableBroadcastState<K, V> previous = (BackendWritableBroadcastState<K, V>) accessedBroadcastStatesByName.get(name);
		if (previous != null) {
			final StateMetaInfo metaInfo = previous.getStateMetaInfo();
			checkStateNameAndMode(
					metaInfo.getName(),
					name,
					OperatorStateHandle.Mode.valueOf(
						metaInfo.getOption(StateMetaInfoBase.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE)),
					OperatorStateHandle.Mode.BROADCAST);
			return previous;
		}

		stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());
		final TypeSerializer<K> broadcastStateKeySerializer =
			Preconditions.checkNotNull(stateDescriptor.getKeySerializer());
		final TypeSerializer<V> broadcastStateValueSerializer =
			Preconditions.checkNotNull(stateDescriptor.getValueSerializer());

		BackendWritableBroadcastState<K, V> broadcastState = (BackendWritableBroadcastState<K, V>) registeredBroadcastStates.get(name);

		final StateMetaInfo stateMetaInfo = StateMetaInfo.Builder.forBroadcastState(name, broadcastStateKeySerializer, broadcastStateValueSerializer);

		if (broadcastState == null) {
			broadcastState = new HeapBroadcastState<>(stateMetaInfo);
			registeredBroadcastStates.put(name, broadcastState);
		} else {
			// has restored state; check compatibility of new state access

			final StateMetaInfo metaInfo = broadcastState.getStateMetaInfo();
			checkStateNameAndMode(
					metaInfo.getName(),
					name,
					OperatorStateHandle.Mode.valueOf(
					metaInfo.getOption(StateMetaInfoBase.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE)),
					OperatorStateHandle.Mode.BROADCAST);

			@SuppressWarnings("unchecked")
			StateMetaInfo.Snapshot restoredMetaInfo = restoredBroadcastStateMetaInfos.get(name);

			// check compatibility to determine if state migration is required
			CompatibilityResult<K> keyCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					restoredMetaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.KEY_SERIALIZER),
					UnloadableDummyTypeSerializer.class,
					restoredMetaInfo.getTypeSerializerConfigSnapshot(StateMetaInfo.CommonSerializerKeys.KEY_SERIALIZER),
					broadcastStateKeySerializer);

			CompatibilityResult<V> valueCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					restoredMetaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER),
					UnloadableDummyTypeSerializer.class,
					restoredMetaInfo.getTypeSerializerConfigSnapshot(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER),
					broadcastStateValueSerializer);

			if (!keyCompatibility.isRequiresMigration() && !valueCompatibility.isRequiresMigration()) {
				// new serializer is compatible; use it to replace the old serializer
				broadcastState.setStateMetaInfo(stateMetaInfo);
			} else {
				// TODO state migration currently isn't possible.

				// NOTE: for heap backends, it is actually fine to proceed here without failing the restore,
				// since the state has already been deserialized to objects and we can just continue with
				// the new serializer; we're deliberately failing here for now to have equal functionality with
				// the RocksDB backend to avoid confusion for users.

				throw new StateMigrationException("State migration isn't supported, yet.");
			}
		}

		accessedBroadcastStatesByName.put(name, broadcastState);
		return broadcastState;
	}

	@Override
	public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
		return getListState(stateDescriptor, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);
	}

	@Override
	public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
		return getListState(stateDescriptor, OperatorStateHandle.Mode.UNION);
	}

	// -------------------------------------------------------------------------------------------
	//  Deprecated state access methods
	// -------------------------------------------------------------------------------------------

	/**
	 * @deprecated This was deprecated as part of a refinement to the function names.
	 *             Please use {@link #getListState(ListStateDescriptor)} instead.
	 */
	@Deprecated
	@Override
	public <S> ListState<S> getOperatorState(ListStateDescriptor<S> stateDescriptor) throws Exception {
		return getListState(stateDescriptor);
	}

	/**
	 * @deprecated Using Java serialization for persisting state is not encouraged.
	 *             Please use {@link #getListState(ListStateDescriptor)} instead.
	 */
	@SuppressWarnings("unchecked")
	@Deprecated
	@Override
	public <T extends Serializable> ListState<T> getSerializableListState(String stateName) throws Exception {
		return (ListState<T>) getListState(new ListStateDescriptor<>(stateName, javaSerializer));
	}

	// -------------------------------------------------------------------------------------------
	//  Snapshot and restore
	// -------------------------------------------------------------------------------------------

	@Override
	public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(
			final long checkpointId,
			final long timestamp,
			final CheckpointStreamFactory streamFactory,
			final CheckpointOptions checkpointOptions) throws Exception {

		final long syncStartTime = System.currentTimeMillis();

		if (registeredOperatorStates.isEmpty() && registeredBroadcastStates.isEmpty()) {
			return DoneFuture.of(SnapshotResult.empty());
		}

		final Map<String, PartitionableListState<?>> registeredOperatorStatesDeepCopies =
				new HashMap<>(registeredOperatorStates.size());
		final Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStatesDeepCopies =
				new HashMap<>(registeredBroadcastStates.size());

		ClassLoader snapshotClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(userClassloader);
		try {
			// eagerly create deep copies of the list and the broadcast states (if any)
			// in the synchronous phase, so that we can use them in the async writing.

			if (!registeredOperatorStates.isEmpty()) {
				for (Map.Entry<String, PartitionableListState<?>> entry : registeredOperatorStates.entrySet()) {
					PartitionableListState<?> listState = entry.getValue();
					if (null != listState) {
						listState = listState.deepCopy();
					}
					registeredOperatorStatesDeepCopies.put(entry.getKey(), listState);
				}
			}

			if (!registeredBroadcastStates.isEmpty()) {
				for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry : registeredBroadcastStates.entrySet()) {
					BackendWritableBroadcastState<?, ?> broadcastState = entry.getValue();
					if (null != broadcastState) {
						broadcastState = broadcastState.deepCopy();
					}
					registeredBroadcastStatesDeepCopies.put(entry.getKey(), broadcastState);
				}
			}
		} finally {
			Thread.currentThread().setContextClassLoader(snapshotClassLoader);
		}

		// implementation of the async IO operation, based on FutureTask
		final AbstractAsyncCallableWithResources<SnapshotResult<OperatorStateHandle>> ioCallable =
			new AbstractAsyncCallableWithResources<SnapshotResult<OperatorStateHandle>>() {

				CheckpointStreamFactory.CheckpointStateOutputStream out = null;

				@Override
				protected void acquireResources() throws Exception {
					openOutStream();
				}

				@Override
				protected void releaseResources() {
					closeOutStream();
				}

				@Override
				protected void stopOperation() {
					closeOutStream();
				}

				private void openOutStream() throws Exception {
					out = streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
					closeStreamOnCancelRegistry.registerCloseable(out);
				}

				private void closeOutStream() {
					if (closeStreamOnCancelRegistry.unregisterCloseable(out)) {
						IOUtils.closeQuietly(out);
					}
				}

				@Nonnull
				@Override
				public SnapshotResult<OperatorStateHandle> performOperation() throws Exception {
					long asyncStartTime = System.currentTimeMillis();

					CheckpointStreamFactory.CheckpointStateOutputStream localOut = this.out;

					// get the registered operator state infos ...
					List<StateMetaInfo.Snapshot> operatorMetaInfoSnapshots =
						new ArrayList<>(registeredOperatorStatesDeepCopies.size());

					for (Map.Entry<String, PartitionableListState<?>> entry : registeredOperatorStatesDeepCopies.entrySet()) {
						operatorMetaInfoSnapshots.add(entry.getValue().getStateMetaInfo().snapshot());
					}

					// ... get the registered broadcast operator state infos ...
					List<StateMetaInfo.Snapshot> broadcastMetaInfoSnapshots =
							new ArrayList<>(registeredBroadcastStatesDeepCopies.size());

					for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry : registeredBroadcastStatesDeepCopies.entrySet()) {
						broadcastMetaInfoSnapshots.add(entry.getValue().getStateMetaInfo().snapshot());
					}

					// ... write them all in the checkpoint stream ...
					DataOutputView dov = new DataOutputViewStreamWrapper(localOut);

					OperatorBackendSerializationProxy backendSerializationProxy =
						new OperatorBackendSerializationProxy(operatorMetaInfoSnapshots, broadcastMetaInfoSnapshots);

					backendSerializationProxy.write(dov);

					// ... and then go for the states ...

					// we put BOTH normal and broadcast state metadata here
					final Map<String, OperatorStateHandle.StateMetaInfo> writtenStatesMetaData =
							new HashMap<>(registeredOperatorStatesDeepCopies.size() + registeredBroadcastStatesDeepCopies.size());

					for (Map.Entry<String, PartitionableListState<?>> entry :
							registeredOperatorStatesDeepCopies.entrySet()) {

						PartitionableListState<?> value = entry.getValue();
						long[] partitionOffsets = value.write(localOut);
						OperatorStateHandle.Mode mode =
							OperatorStateHandle.Mode.valueOf(
								value.getStateMetaInfo().getOption(
									StateMetaInfo.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE));
						writtenStatesMetaData.put(
							entry.getKey(),
							new OperatorStateHandle.StateMetaInfo(partitionOffsets, mode));
					}

					// ... and the broadcast states themselves ...
					for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry :
							registeredBroadcastStatesDeepCopies.entrySet()) {

						BackendWritableBroadcastState<?, ?> value = entry.getValue();
						long[] partitionOffsets = {value.write(localOut)};
						OperatorStateHandle.Mode mode =
							OperatorStateHandle.Mode.valueOf(
								value.getStateMetaInfo().getOption(
									StateMetaInfo.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE));
						writtenStatesMetaData.put(
								entry.getKey(),
								new OperatorStateHandle.StateMetaInfo(partitionOffsets, mode));
					}

					// ... and, finally, create the state handle.
					OperatorStateHandle retValue = null;

					if (closeStreamOnCancelRegistry.unregisterCloseable(out)) {

						StreamStateHandle stateHandle = out.closeAndGetHandle();

						if (stateHandle != null) {
							retValue = new OperatorStreamStateHandle(writtenStatesMetaData, stateHandle);
						}
					}

					if (asynchronousSnapshots) {
						LOG.info("DefaultOperatorStateBackend snapshot ({}, asynchronous part) in thread {} took {} ms.",
							streamFactory, Thread.currentThread(), (System.currentTimeMillis() - asyncStartTime));
					}

					return SnapshotResult.of(retValue);
				}
			};

		AsyncStoppableTaskWithCallback<SnapshotResult<OperatorStateHandle>> task =
			AsyncStoppableTaskWithCallback.from(ioCallable);

		if (!asynchronousSnapshots) {
			task.run();
		}

		LOG.info("DefaultOperatorStateBackend snapshot ({}, synchronous part) in thread {} took {} ms.",
				streamFactory, Thread.currentThread(), (System.currentTimeMillis() - syncStartTime));

		return task;
	}

	public void restore(Collection<OperatorStateHandle> restoreSnapshots) throws Exception {

		if (null == restoreSnapshots || restoreSnapshots.isEmpty()) {
			return;
		}

		for (OperatorStateHandle stateHandle : restoreSnapshots) {

			if (stateHandle == null) {
				continue;
			}

			FSDataInputStream in = stateHandle.openInputStream();
			closeStreamOnCancelRegistry.registerCloseable(in);

			ClassLoader restoreClassLoader = Thread.currentThread().getContextClassLoader();

			try {
				Thread.currentThread().setContextClassLoader(userClassloader);
				OperatorBackendSerializationProxy backendSerializationProxy =
						new OperatorBackendSerializationProxy(userClassloader);

				backendSerializationProxy.read(new DataInputViewStreamWrapper(in));

				List<StateMetaInfo.Snapshot> restoredOperatorMetaInfoSnapshots =
						backendSerializationProxy.getOperatorStateMetaInfoSnapshots();

				// Recreate all PartitionableListStates from the meta info
				for (StateMetaInfo.Snapshot restoredMetaInfoSnapshot : restoredOperatorMetaInfoSnapshots) {

					final TypeSerializer<?> valueSerializer =
						restoredMetaInfoSnapshot.getTypeSerializer(
							StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER);

					if (valueSerializer == null || valueSerializer instanceof UnloadableDummyTypeSerializer) {

						// must fail now if the previous serializer cannot be restored because there is no serializer
						// capable of reading previous state
						// TODO when eager state registration is in place, we can try to get a convert deserializer
						// TODO from the newly registered serializer instead of simply failing here

						throw new IOException("Unable to restore operator state [" + restoredMetaInfoSnapshot.getName() + "]." +
							" The previous serializer of the operator state must be present; the serializer could" +
							" have been removed from the classpath, or its implementation have changed and could" +
							" not be loaded. This is a temporary restriction that will be fixed in future versions.");
					}

					restoredOperatorStateMetaInfos.put(restoredMetaInfoSnapshot.getName(), restoredMetaInfoSnapshot);

					PartitionableListState<?> listState = registeredOperatorStates.get(restoredMetaInfoSnapshot.getName());

					if (null == listState) {

						final StateMetaInfo stateMetaInfo = new StateMetaInfo(
							restoredMetaInfoSnapshot.getName(),
							new HashMap<>(restoredMetaInfoSnapshot.getOptionsImmutable()));

						stateMetaInfo.setTypeSerializer(
							StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER,
							valueSerializer);

						listState = new PartitionableListState<>(stateMetaInfo);

						registeredOperatorStates.put(listState.getStateMetaInfo().getName(), listState);
					} else {
						// TODO with eager state registration in place, check here for serializer migration strategies
					}
				}

				// ... and then get back the broadcast state.
				List<StateMetaInfo.Snapshot> restoredBroadcastMetaInfoSnapshots =
						backendSerializationProxy.getBroadcastStateMetaInfoSnapshots();

				for (StateMetaInfo.Snapshot restoredMetaInfoSnapshot : restoredBroadcastMetaInfoSnapshots) {

					final TypeSerializer<?> valueSerializer =
						restoredMetaInfoSnapshot.getTypeSerializer(
							StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER);

					final TypeSerializer<?> keySerializer =
						restoredMetaInfoSnapshot.getTypeSerializer(
							StateMetaInfo.CommonSerializerKeys.KEY_SERIALIZER);

					if (keySerializer == null || valueSerializer == null ||
							keySerializer instanceof UnloadableDummyTypeSerializer ||
							valueSerializer instanceof UnloadableDummyTypeSerializer) {

						// must fail now if the previous serializer cannot be restored because there is no serializer
						// capable of reading previous state
						// TODO when eager state registration is in place, we can try to get a convert deserializer
						// TODO from the newly registered serializer instead of simply failing here

						throw new IOException("Unable to restore broadcast state [" + restoredMetaInfoSnapshot.getName() + "]." +
								" The previous key and value serializers of the state must be present; the serializers could" +
								" have been removed from the classpath, or their implementations have changed and could" +
								" not be loaded. This is a temporary restriction that will be fixed in future versions.");
					}

					restoredBroadcastStateMetaInfos.put(restoredMetaInfoSnapshot.getName(), restoredMetaInfoSnapshot);

					BackendWritableBroadcastState<? ,?> broadcastState = registeredBroadcastStates.get(restoredMetaInfoSnapshot.getName());

					if (broadcastState == null) {
						broadcastState = new HeapBroadcastState<>(restoredMetaInfoSnapshot.toStateMetaInfo());
						registeredBroadcastStates.put(broadcastState.getStateMetaInfo().getName(), broadcastState);
					} else {
						// TODO with eager state registration in place, check here for serializer migration strategies
					}
				}

				// Restore all the states
				for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> nameToOffsets :
						stateHandle.getStateNameToPartitionOffsets().entrySet()) {

					final String stateName = nameToOffsets.getKey();

					PartitionableListState<?> listStateForName = registeredOperatorStates.get(stateName);
					if (listStateForName == null) {
						BackendWritableBroadcastState<?, ?> broadcastStateForName = registeredBroadcastStates.get(stateName);
						Preconditions.checkState(broadcastStateForName != null, "Found state without " +
								"corresponding meta info: " + stateName);
						deserializeBroadcastStateValues(broadcastStateForName, in, nameToOffsets.getValue());
					} else {
						deserializeOperatorStateValues(listStateForName, in, nameToOffsets.getValue());
					}
				}

			} finally {
				Thread.currentThread().setContextClassLoader(restoreClassLoader);
				if (closeStreamOnCancelRegistry.unregisterCloseable(in)) {
					IOUtils.closeQuietly(in);
				}
			}
		}
	}

	/**
	 * Implementation of operator list state.
	 *
	 * @param <S> the type of an operator state partition.
	 */
	static final class PartitionableListState<S> implements ListState<S> {

		/**
		 * Meta information of the state, including state name, assignment mode, and serializer
		 */
		private StateMetaInfo stateMetaInfo;

		/**
		 * The internal list the holds the elements of the state
		 */
		private final ArrayList<S> internalList;

		/**
		 * A serializer that allows to perform deep copies of internalList
		 */
		private ArrayListSerializer<S> internalListCopySerializer;

		PartitionableListState(StateMetaInfo stateMetaInfo) {
			this(stateMetaInfo, new ArrayList<>());
		}


		private PartitionableListState(
				StateMetaInfo stateMetaInfo,
				ArrayList<S> internalList) {

			this.internalList = Preconditions.checkNotNull(internalList);
			setStateMetaInfo(stateMetaInfo);
		}

		private PartitionableListState(PartitionableListState<S> toCopy) {

			this(toCopy.stateMetaInfo.deepCopy(), toCopy.internalListCopySerializer.copy(toCopy.internalList));
		}

		@SuppressWarnings("unchecked")
		public void setStateMetaInfo(StateMetaInfo stateMetaInfo) {
			this.stateMetaInfo = Preconditions.checkNotNull(stateMetaInfo);
			this.internalListCopySerializer = new ArrayListSerializer<>(
				(TypeSerializer<S>) stateMetaInfo.getTypeSerializer(
					StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER));
		}

		public StateMetaInfo getStateMetaInfo() {
			return stateMetaInfo;
		}

		public PartitionableListState<S> deepCopy() {
			return new PartitionableListState<>(this);
		}

		@Override
		public void clear() {
			internalList.clear();
		}

		@Override
		public Iterable<S> get() {
			return internalList;
		}

		@Override
		public void add(S value) {
			Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
			internalList.add(value);
		}

		@Override
		public String toString() {
			return "PartitionableListState{" +
					"stateMetaInfo=" + stateMetaInfo +
					", internalList=" + internalList +
					'}';
		}

		@SuppressWarnings("unchecked")
		public long[] write(FSDataOutputStream out) throws IOException {

			long[] partitionOffsets = new long[internalList.size()];

			DataOutputView dov = new DataOutputViewStreamWrapper(out);
			final StateMetaInfo stateMetaInfo = getStateMetaInfo();
			final TypeSerializer<S> valueSerializer = (TypeSerializer<S>) Preconditions.checkNotNull(
				stateMetaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER));

			for (int i = 0; i < internalList.size(); ++i) {
				S element = internalList.get(i);
				partitionOffsets[i] = out.getPos();
				valueSerializer.serialize(element, dov);
			}

			return partitionOffsets;
		}

		@Override
		public void update(List<S> values) {
			internalList.clear();

			addAll(values);
		}

		@Override
		public void addAll(List<S> values) {
			if (values != null && !values.isEmpty()) {
				internalList.addAll(values);
			}
		}
	}

	private <S> ListState<S> getListState(
			ListStateDescriptor<S> stateDescriptor,
			OperatorStateHandle.Mode mode) throws StateMigrationException {

		Preconditions.checkNotNull(stateDescriptor);
		String name = Preconditions.checkNotNull(stateDescriptor.getName());

		@SuppressWarnings("unchecked")
		PartitionableListState<S> previous = (PartitionableListState<S>) accessedStatesByName.get(name);
		if (previous != null) {
			final StateMetaInfo stateMetaInfo = previous.getStateMetaInfo();
			checkStateNameAndMode(
					stateMetaInfo.getName(),
					name,
					OperatorStateHandle.Mode.valueOf(
						stateMetaInfo.getOption(StateMetaInfoBase.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE)),
					mode);
			return previous;
		}

		// end up here if its the first time access after execution for the
		// provided state name; check compatibility of restored state, if any
		// TODO with eager registration in place, these checks should be moved to restore()

		stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());
		TypeSerializer<S> partitionStateSerializer = Preconditions.checkNotNull(stateDescriptor.getElementSerializer());

		@SuppressWarnings("unchecked")
		PartitionableListState<S> partitionableListState = (PartitionableListState<S>) registeredOperatorStates.get(name);

		if (null == partitionableListState) {
			// no restored state for the state name; simply create new state holder

			final StateMetaInfo stateMetaInfo =
				StateMetaInfo.Builder.forOperatorState(name, partitionStateSerializer, mode);

			partitionableListState = new PartitionableListState<>(stateMetaInfo);

			registeredOperatorStates.put(name, partitionableListState);
		} else {
			// has restored state; check compatibility of new state access

			final StateMetaInfo stateMetaInfo = partitionableListState.getStateMetaInfo();
			checkStateNameAndMode(
					stateMetaInfo.getName(),
					name,
					OperatorStateHandle.Mode.valueOf(
					stateMetaInfo.getOption(StateMetaInfoBase.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE)),
					mode);

			StateMetaInfo.Snapshot restoredMetaInfo = restoredOperatorStateMetaInfos.get(name);

			// check compatibility to determine if state migration is required
			TypeSerializer<S> newPartitionStateSerializer = partitionStateSerializer.duplicate();
			CompatibilityResult<S> stateCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					restoredMetaInfo.getTypeSerializer(
						StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER),
					UnloadableDummyTypeSerializer.class,
					restoredMetaInfo.getTypeSerializerConfigSnapshot(
						StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER),
					newPartitionStateSerializer);

			if (!stateCompatibility.isRequiresMigration()) {
				stateMetaInfo.setTypeSerializer(
					StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER,
					newPartitionStateSerializer);

				// new serializer is compatible; use it to replace the old serializer
				partitionableListState.setStateMetaInfo(stateMetaInfo);
			} else {
				// TODO state migration currently isn't possible.

				// NOTE: for heap backends, it is actually fine to proceed here without failing the restore,
				// since the state has already been deserialized to objects and we can just continue with
				// the new serializer; we're deliberately failing here for now to have equal functionality with
				// the RocksDB backend to avoid confusion for users.

				throw new StateMigrationException("State migration isn't supported, yet.");
			}
		}

		accessedStatesByName.put(name, partitionableListState);
		return partitionableListState;
	}

	private static <S> void deserializeOperatorStateValues(
		PartitionableListState<S> stateListForName,
		FSDataInputStream in,
		OperatorStateHandle.StateMetaInfo metaInfo) throws IOException {

		if (null != metaInfo) {
			long[] offsets = metaInfo.getOffsets();
			if (null != offsets) {
				DataInputView div = new DataInputViewStreamWrapper(in);
				TypeSerializer<S> serializer = stateListForName.internalListCopySerializer.getElementSerializer();
				for (long offset : offsets) {
					in.seek(offset);
					stateListForName.add(serializer.deserialize(div));
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static <K, V> void deserializeBroadcastStateValues(
			final BackendWritableBroadcastState<K, V> broadcastStateForName,
			final FSDataInputStream in,
			final OperatorStateHandle.StateMetaInfo metaInfo) throws Exception {

		if (metaInfo != null) {
			long[] offsets = metaInfo.getOffsets();
			if (offsets != null) {
				final StateMetaInfo stateMetaInfo = broadcastStateForName.getStateMetaInfo();
				TypeSerializer<K> keySerializer = (TypeSerializer<K>) Preconditions.checkNotNull(
					stateMetaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.KEY_SERIALIZER));
				TypeSerializer<V> valueSerializer = (TypeSerializer<V>) Preconditions.checkNotNull(
					stateMetaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER));

				in.seek(offsets[0]);

				DataInputView div = new DataInputViewStreamWrapper(in);
				int size = div.readInt();
				for (int i = 0; i < size; i++) {
					broadcastStateForName.put(keySerializer.deserialize(div), valueSerializer.deserialize(div));
				}
			}
		}
	}

	private static void checkStateNameAndMode(
			String actualName,
			String expectedName,
			OperatorStateHandle.Mode actualMode,
			OperatorStateHandle.Mode expectedMode) {

		Preconditions.checkState(
			actualName.equals(expectedName),
			"Incompatible state names. " +
				"Was [" + actualName + "], " +
				"registered with [" + expectedName + "].");

		Preconditions.checkState(
				actualMode.equals(expectedMode),
			"Incompatible state assignment modes. " +
				"Was [" + actualMode + "], " +
				"registered with [" + expectedMode + "].");
	}
}
