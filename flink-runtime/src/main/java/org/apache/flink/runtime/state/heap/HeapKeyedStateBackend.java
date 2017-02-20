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

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.IOUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.migration.MigrationUtil;
import org.apache.flink.migration.runtime.state.KvStateSnapshot;
import org.apache.flink.migration.runtime.state.memory.MigrationRestoreSnapshot;
import org.apache.flink.runtime.io.async.AbstractAsyncIOCallable;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link AbstractKeyedStateBackend} that keeps state on the Java Heap and will serialize state to
 * streams provided by a {@link org.apache.flink.runtime.state.CheckpointStreamFactory} upon
 * checkpointing.
 *
 * @param <K> The key by which state is keyed.
 */
public class HeapKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(HeapKeyedStateBackend.class);
	private final Object snapshotLock = new Object();

	/**
	 * Map of state tables that stores all state of key/value states. We store it centrally so
	 * that we can easily checkpoint/restore it.
	 *
	 * <p>The actual parameters of StateTable are {@code StateTable<NamespaceT, Map<KeyT, StateT>>}
	 * but we can't put them here because different key/value states with different types and
	 * namespace types share this central list of tables.
	 */
	private final HashMap<String, StateTable<K, ?, ?>> stateTables = new HashMap<>();

	public HeapKeyedStateBackend(
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			ClassLoader userCodeClassLoader,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange) {

		super(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups, keyGroupRange);
		LOG.info("Initializing heap keyed state backend with stream factory.");
	}

	// ------------------------------------------------------------------------
	//  state backend operations
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private <N, V> StateTable<K, N, V> tryRegisterStateTable(
			TypeSerializer<N> namespaceSerializer, StateDescriptor<?, V> stateDesc) {

		String name = stateDesc.getName();
		StateTable<K, N, V> stateTable = (StateTable<K, N, V>) stateTables.get(name);

		RegisteredBackendStateMetaInfo<N, V> newMetaInfo =
				new RegisteredBackendStateMetaInfo<>(stateDesc.getType(), name, namespaceSerializer, stateDesc.getSerializer());

		return tryRegisterStateTable(stateTable, newMetaInfo);
	}

	private <N, V> StateTable<K, N, V> tryRegisterStateTable(
			StateTable<K, N, V> stateTable, RegisteredBackendStateMetaInfo<N, V> newMetaInfo) {

		if (stateTable == null) {
			stateTable = new StateTable<>(newMetaInfo);
			stateTables.put(newMetaInfo.getName(), stateTable);
		} else {
			if (!newMetaInfo.isCompatibleWith(stateTable.getMetaInfo())) {
				throw new RuntimeException("Trying to access state using incompatible meta info, was " +
						stateTable.getMetaInfo() + " trying access with " + newMetaInfo);
			}
			stateTable.setMetaInfo(newMetaInfo);
		}
		return stateTable;
	}

	@Override
	public <N, V> InternalValueState<N, V> createValueState(
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<V> stateDesc) throws Exception {

		StateTable<K, N, V> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return new HeapValueState<>(this, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public <N, T> InternalListState<N, T> createListState(
			TypeSerializer<N> namespaceSerializer,
			ListStateDescriptor<T> stateDesc) throws Exception {

		String name = stateDesc.getName();

		@SuppressWarnings("unchecked")
		StateTable<K, N, ArrayList<T>> stateTable = (StateTable<K, N, ArrayList<T>>) stateTables.get(name);

		RegisteredBackendStateMetaInfo<N, ArrayList<T>> newMetaInfo =
				new RegisteredBackendStateMetaInfo<>(stateDesc.getType(), name, namespaceSerializer, new ArrayListSerializer<>(stateDesc.getSerializer()));

		stateTable = tryRegisterStateTable(stateTable, newMetaInfo);
		return new HeapListState<>(this, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public <N, T> InternalReducingState<N, T> createReducingState(
			TypeSerializer<N> namespaceSerializer,
			ReducingStateDescriptor<T> stateDesc) throws Exception {

		StateTable<K, N, T> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return new HeapReducingState<>(this, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public <N, T, ACC, R> InternalAggregatingState<N, T, R> createAggregatingState(
			TypeSerializer<N> namespaceSerializer,
			AggregatingStateDescriptor<T, ACC, R> stateDesc) throws Exception {

		StateTable<K, N, ACC> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return new HeapAggregatingState<>(this, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	protected <N, T, ACC> InternalFoldingState<N, T, ACC> createFoldingState(
			TypeSerializer<N> namespaceSerializer,
			FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {

		StateTable<K, N, ACC> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return new HeapFoldingState<>(this, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	@SuppressWarnings("unchecked")
	public RunnableFuture<KeyGroupsStateHandle> snapshot(
			final long checkpointId,
			final long timestamp,
			final CheckpointStreamFactory streamFactory) throws Exception {

		if (stateTables.isEmpty()) {
			return new DoneFuture<>(null);
		}

		long syncStartTime = System.currentTimeMillis();

		Preconditions.checkState(stateTables.size() <= Short.MAX_VALUE,
				"Too many KV-States: " + stateTables.size() +
						". Currently at most " + Short.MAX_VALUE + " states are supported");

		List<KeyedBackendSerializationProxy.StateMetaInfo<?, ?>> metaInfoProxyList = new ArrayList<>(stateTables.size());

		final Map<String, Integer> kVStateToId = new HashMap<>(stateTables.size());

		final Map<StateTable<K, ?, ?>, StateTableSnapshot<K, ?, ?>> cowStateStableSnapshots = new HashedMap(stateTables.size());

			for (Map.Entry<String, StateTable<K, ?, ?>> kvState : stateTables.entrySet()) {
				RegisteredBackendStateMetaInfo<?, ?> metaInfo = kvState.getValue().getMetaInfo();
				KeyedBackendSerializationProxy.StateMetaInfo<?, ?> metaInfoProxy = new KeyedBackendSerializationProxy.StateMetaInfo(
						metaInfo.getStateType(),
						metaInfo.getName(),
						metaInfo.getNamespaceSerializer(),
						metaInfo.getStateSerializer());

				metaInfoProxyList.add(metaInfoProxy);
				kVStateToId.put(kvState.getKey(), kVStateToId.size());
				StateTable<K, ?, ?> stateTable = kvState.getValue();
				if (null != stateTable) {
					cowStateStableSnapshots.put(stateTable, stateTable.createSnapshot(keySerializer, keyGroupRange, getNumberOfKeyGroups()));
				}
		}

		final KeyedBackendSerializationProxy serializationProxy =
				new KeyedBackendSerializationProxy(keySerializer, metaInfoProxyList);

		//--------------------------------------------------- this becomes the end of sync part

		// implementation of the async IO operation, based on FutureTask
		final AbstractAsyncIOCallable<KeyGroupsStateHandle, CheckpointStreamFactory.CheckpointStateOutputStream> ioCallable =
				new AbstractAsyncIOCallable<KeyGroupsStateHandle, CheckpointStreamFactory.CheckpointStateOutputStream>() {

					AtomicBoolean open = new AtomicBoolean(false);

					@Override
					public CheckpointStreamFactory.CheckpointStateOutputStream openIOHandle() throws Exception {
						if (open.compareAndSet(false, true)) {
							CheckpointStreamFactory.CheckpointStateOutputStream stream =
									streamFactory.createCheckpointStateOutputStream(checkpointId, timestamp);
							try {
								cancelStreamRegistry.registerClosable(stream);
								return stream;
							} catch (Exception ex) {
								open.set(false);
								throw ex;
							}
						} else {
							throw new IOException("Operation already opened.");
						}
					}

					@Override
					public KeyGroupsStateHandle performOperation() throws Exception {
						long asyncStartTime = System.currentTimeMillis();
						CheckpointStreamFactory.CheckpointStateOutputStream stream = getIoHandle();
						DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(stream);
						serializationProxy.write(outView);

						long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];

						for (int keyGroupPos = 0; keyGroupPos < keyGroupRange.getNumberOfKeyGroups(); ++keyGroupPos) {
							int keyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
							keyGroupRangeOffsets[keyGroupPos] = stream.getPos();
							outView.writeInt(keyGroupId);

							for (Map.Entry<String, StateTable<K, ?, ?>> kvState : stateTables.entrySet()) {
								outView.writeShort(kVStateToId.get(kvState.getKey()));
								cowStateStableSnapshots.get(kvState.getValue()).writeKeyGroupData(outView, keyGroupId);
							}
						}

						StreamStateHandle streamStateHandle = stream.closeAndGetHandle();
						open.set(false);
						KeyGroupRangeOffsets offsets = new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets);
						final KeyGroupsStateHandle keyGroupsStateHandle = new KeyGroupsStateHandle(offsets, streamStateHandle);
						LOG.info("Asynchronous heap backend  snapshot ({}, asynchronous part) in thread {} took {} ms.",
								streamFactory, Thread.currentThread(), (System.currentTimeMillis() - asyncStartTime));

						return keyGroupsStateHandle;
					}

					@Override
					public void done(boolean canceled) {
						if (open.compareAndSet(true, false)) {
							CheckpointStreamFactory.CheckpointStateOutputStream stream = getIoHandle();
							if (null != stream) {
								cancelStreamRegistry.unregisterClosable(stream);
								IOUtils.closeQuietly(stream);
							}
						}
						for (Map.Entry<StateTable<K, ?, ?>, StateTableSnapshot<K, ?, ?>> entry
							: cowStateStableSnapshots.entrySet()) {
							int snapshotVersion = entry.getValue().getSnapshotVersion();
							entry.getKey().releaseSnapshot(snapshotVersion);
						}
					}
				};

		LOG.info("Asynchronous heap backend snapshot (" + streamFactory + ", synchronous part) in thread " +
				Thread.currentThread() + " took " + (System.currentTimeMillis() - syncStartTime) + " ms.");

		return AsyncStoppableTaskWithCallback.from(ioCallable);
	}

	@SuppressWarnings("deprecation")
	@Override
	public void restore(Collection<KeyGroupsStateHandle> restoredState) throws Exception {
		LOG.info("Initializing heap keyed state backend from snapshot.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Restoring snapshot from state handles: {}.", restoredState);
		}

		if (MigrationUtil.isOldSavepointKeyedState(restoredState)) {
			restoreOldSavepointKeyedState(restoredState);
		} else {
			restorePartitionedState(restoredState);
		}
	}

	@SuppressWarnings({"unchecked"})
	private void restorePartitionedState(Collection<KeyGroupsStateHandle> state) throws Exception {

		int numRegisteredKvStates = 0;
		Map<Integer, String> kvStatesById = new HashMap<>();
		stateTables.clear();

		for (KeyGroupsStateHandle keyGroupsHandle : state) {

			if (keyGroupsHandle == null) {
				continue;
			}

			FSDataInputStream fsDataInputStream = keyGroupsHandle.openInputStream();
			cancelStreamRegistry.registerClosable(fsDataInputStream);

			try {
				DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(fsDataInputStream);

				KeyedBackendSerializationProxy serializationProxy =
						new KeyedBackendSerializationProxy(userCodeClassLoader);

				serializationProxy.read(inView);

				List<KeyedBackendSerializationProxy.StateMetaInfo<?, ?>> metaInfoList =
						serializationProxy.getNamedStateSerializationProxies();

				for (KeyedBackendSerializationProxy.StateMetaInfo<?, ?> metaInfoSerializationProxy : metaInfoList) {

					StateTable<K, ?, ?> stateTable = stateTables.get(metaInfoSerializationProxy.getStateName());

					//important: only create a new table we did not already create it previously
					if (null == stateTable) {

						RegisteredBackendStateMetaInfo<?, ?> registeredBackendStateMetaInfo =
								new RegisteredBackendStateMetaInfo<>(metaInfoSerializationProxy);

						stateTable = new StateTable<>(registeredBackendStateMetaInfo);
						stateTables.put(metaInfoSerializationProxy.getStateName(), stateTable);
						kvStatesById.put(numRegisteredKvStates, metaInfoSerializationProxy.getStateName());
						++numRegisteredKvStates;
					}
				}

				for (Tuple2<Integer, Long> groupOffset : keyGroupsHandle.getGroupRangeOffsets()) {
					int keyGroupIndex = groupOffset.f0;
					long offset = groupOffset.f1;
					fsDataInputStream.seek(offset);

					int writtenKeyGroupIndex = inView.readInt();

					Preconditions.checkState(writtenKeyGroupIndex == keyGroupIndex,
							"Unexpected keygroup in restore.");

					for (int i = 0; i < metaInfoList.size(); i++) {
						int kvStateId = inView.readShort();
						StateTable<K, ?, ?> stateTable = stateTables.get(kvStatesById.get(kvStateId));
						Preconditions.checkNotNull(stateTable);
						readStateTableForKeyGroup(inView, stateTable);
					}
				}
			} finally {
				cancelStreamRegistry.unregisterClosable(fsDataInputStream);
				IOUtils.closeQuietly(fsDataInputStream);
			}
		}
	}

	private <N, S> void readStateTableForKeyGroup(
			DataInputView inView,
			StateTable<K, N, S> stateTable) throws IOException {

		TypeSerializer<N> namespaceSerializer = stateTable.getNamespaceSerializer();
		TypeSerializer<S> stateSerializer = stateTable.getStateSerializer();

		int numKeys = inView.readInt();
		for (int i = 0; i < numKeys; ++i) {
			K key = keySerializer.deserialize(inView);
			N namespace = namespaceSerializer.deserialize(inView);
			S state = stateSerializer.deserialize(inView);
			stateTable.put(key, namespace, state);
		}
	}

	@Override
	public String toString() {
		return "HeapKeyedStateBackend";
	}

	@SuppressWarnings({"unchecked", "rawtypes", "DeprecatedIsStillUsed"})
	@Deprecated
	private void restoreOldSavepointKeyedState(
			Collection<KeyGroupsStateHandle> stateHandles) throws IOException, ClassNotFoundException {

		if (stateHandles.isEmpty()) {
			return;
		}

		Preconditions.checkState(1 == stateHandles.size(), "Only one element expected here.");

		HashMap<String, KvStateSnapshot<K, ?, ?, ?>> namedStates;
		try (FSDataInputStream inputStream = stateHandles.iterator().next().openInputStream()) {
			namedStates = InstantiationUtil.deserializeObject(inputStream, userCodeClassLoader);
		}

		for (Map.Entry<String, KvStateSnapshot<K, ?, ?, ?>> nameToState : namedStates.entrySet()) {

			final String stateName = nameToState.getKey();
			final KvStateSnapshot<K, ?, ?, ?> genericSnapshot = nameToState.getValue();

			if (genericSnapshot instanceof MigrationRestoreSnapshot) {
				MigrationRestoreSnapshot<K, ?, ?> stateSnapshot = (MigrationRestoreSnapshot<K, ?, ?>) genericSnapshot;
				final StateTable rawResultMap = stateSnapshot.deserialize(stateName, keyGroupRange, numberOfKeyGroups);
				// add named state to the backend
				stateTables.put(stateName, rawResultMap);
			} else {
				throw new IllegalStateException("Unknown state: " + genericSnapshot);
			}
		}
	}

	/**
	 * Returns the total number of state entries across all keys/namespaces.
	 */
	@VisibleForTesting
	@SuppressWarnings("unchecked")
	public int numStateEntries() {
		int sum = 0;
		for (StateTable<K, ?, ?> stateTable : stateTables.values()) {
			sum += stateTable.size();

		}
		return sum;
	}

	/**
	 * Returns the total number of state entries across all keys for the given namespace.
	 */
	@VisibleForTesting
	@SuppressWarnings("unchecked")
	public <N> int numStateEntries(N namespace) {
		int sum = 0;
		for (StateTable<K, ?, ?> stateTable : stateTables.values()) {
			for (StateEntry<K, ?, ?> entry : stateTable) {
				if (namespace.equals(entry.getNamespace())) {
					++sum;
				}
			}
		}
		return sum;
	}
}
