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

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.metainfo.StateMetaInfo;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoBase;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoReader;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters;
import org.apache.flink.testutils.ArtificialCNFExceptionThrowingClassLoader;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters.CURRENT_STATE_META_INFO_SNAPSHOT_VERSION;

public class SerializationProxiesTest {

	@Test
	public void testKeyedBackendSerializationProxyRoundtrip() throws Exception {

		TypeSerializer<?> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		List<StateMetaInfo.Snapshot> stateMetaInfoList = new ArrayList<>();

		stateMetaInfoList.add(StateMetaInfo.Builder.forKeyedState(
			"a", StateDescriptor.Type.VALUE, namespaceSerializer, stateSerializer).snapshot());
		stateMetaInfoList.add(StateMetaInfo.Builder.forKeyedState(
			"b", StateDescriptor.Type.VALUE, namespaceSerializer, stateSerializer).snapshot());
		stateMetaInfoList.add(StateMetaInfo.Builder.forKeyedState(
			"c", StateDescriptor.Type.VALUE, namespaceSerializer, stateSerializer).snapshot());

		KeyedBackendSerializationProxy<?> serializationProxy =
				new KeyedBackendSerializationProxy<>(keySerializer, stateMetaInfoList, true);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			serializationProxy.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		serializationProxy =
				new KeyedBackendSerializationProxy<>(Thread.currentThread().getContextClassLoader(), true);

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			serializationProxy.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(true, serializationProxy.isUsingKeyGroupCompression());
		Assert.assertEquals(keySerializer, serializationProxy.getKeySerializer());
		Assert.assertEquals(keySerializer.snapshotConfiguration(), serializationProxy.getKeySerializerConfigSnapshot());
		Assert.assertEquals(stateMetaInfoList, serializationProxy.getStateMetaInfoSnapshots());
	}

	@Test
	public void testKeyedBackendSerializationProxyRoundtripWithSerializerSerializationFailures() throws Exception {

		TypeSerializer<?> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		List<StateMetaInfo.Snapshot> stateMetaInfoList = new ArrayList<>();

		stateMetaInfoList.add(StateMetaInfo.Builder.forKeyedState(
			"a", StateDescriptor.Type.VALUE, namespaceSerializer, stateSerializer).snapshot());
		stateMetaInfoList.add(StateMetaInfo.Builder.forKeyedState(
			"b", StateDescriptor.Type.VALUE, namespaceSerializer, stateSerializer).snapshot());
		stateMetaInfoList.add(StateMetaInfo.Builder.forKeyedState(
			"c", StateDescriptor.Type.VALUE, namespaceSerializer, stateSerializer).snapshot());

		KeyedBackendSerializationProxy<?> serializationProxy =
			new KeyedBackendSerializationProxy<>(keySerializer, stateMetaInfoList, true);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			serializationProxy.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		Set<String> cnfThrowingSerializerClasses = new HashSet<>();
		cnfThrowingSerializerClasses.add(IntSerializer.class.getName());
		cnfThrowingSerializerClasses.add(LongSerializer.class.getName());
		cnfThrowingSerializerClasses.add(DoubleSerializer.class.getName());

		// we want to verify restore resilience when serializer presence is not required;
		// set isSerializerPresenceRequired to false
		serializationProxy =
			new KeyedBackendSerializationProxy<>(
				new ArtificialCNFExceptionThrowingClassLoader(
					Thread.currentThread().getContextClassLoader(),
					cnfThrowingSerializerClasses),
				false);

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			serializationProxy.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(true, serializationProxy.isUsingKeyGroupCompression());
		Assert.assertTrue(serializationProxy.getKeySerializer() instanceof UnloadableDummyTypeSerializer);
		Assert.assertEquals(keySerializer.snapshotConfiguration(), serializationProxy.getKeySerializerConfigSnapshot());

		for (StateMetaInfo.Snapshot meta : serializationProxy.getStateMetaInfoSnapshots()) {
			Assert.assertTrue(meta.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.NAMESPACE_SERIALIZER) instanceof UnloadableDummyTypeSerializer);
			Assert.assertTrue(meta.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER) instanceof UnloadableDummyTypeSerializer);
			Assert.assertEquals(namespaceSerializer.snapshotConfiguration(), meta.getTypeSerializerConfigSnapshot(StateMetaInfo.CommonSerializerKeys.NAMESPACE_SERIALIZER));
			Assert.assertEquals(stateSerializer.snapshotConfiguration(), meta.getTypeSerializerConfigSnapshot(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER));
		}
	}

	@Test
	public void testKeyedStateMetaInfoSerialization() throws Exception {

		String name = "test";
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		StateMetaInfo.Snapshot metaInfo = StateMetaInfo.Builder.forKeyedState(
			name, StateDescriptor.Type.VALUE, namespaceSerializer, stateSerializer).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			StateMetaInfoSnapshotReadersWriters.getWriter().
				writeStateMetaInfoSnapshot(metaInfo, new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			final StateMetaInfoReader reader = StateMetaInfoSnapshotReadersWriters.getReader(
				CURRENT_STATE_META_INFO_SNAPSHOT_VERSION, StateMetaInfoSnapshotReadersWriters.StateTypeHint.KEYED_STATE);
			metaInfo = reader.readStateMetaInfoSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		Assert.assertEquals(name, metaInfo.getName());
	}

	@Test
	public void testKeyedStateMetaInfoReadSerializerFailureResilience() throws Exception {
		String name = "test";
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		StateMetaInfo.Snapshot metaInfo = StateMetaInfo.Builder.forKeyedState(
			name, StateDescriptor.Type.VALUE, namespaceSerializer, stateSerializer).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			StateMetaInfoSnapshotReadersWriters.getWriter().
				writeStateMetaInfoSnapshot(metaInfo, new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		Set<String> cnfThrowingSerializerClasses = new HashSet<>();
		cnfThrowingSerializerClasses.add(LongSerializer.class.getName());
		cnfThrowingSerializerClasses.add(DoubleSerializer.class.getName());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			final StateMetaInfoReader reader = StateMetaInfoSnapshotReadersWriters.getReader(
				CURRENT_STATE_META_INFO_SNAPSHOT_VERSION, StateMetaInfoSnapshotReadersWriters.StateTypeHint.KEYED_STATE);
			final ClassLoader classLoader = new ArtificialCNFExceptionThrowingClassLoader(
				Thread.currentThread().getContextClassLoader(),
				cnfThrowingSerializerClasses);

			metaInfo = reader.readStateMetaInfoSnapshot(
				new DataInputViewStreamWrapper(in), classLoader);
		}

		Assert.assertEquals(name, metaInfo.getName());
		Assert.assertTrue(metaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.NAMESPACE_SERIALIZER) instanceof UnloadableDummyTypeSerializer);
		Assert.assertTrue(metaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER) instanceof UnloadableDummyTypeSerializer);
		Assert.assertEquals(namespaceSerializer.snapshotConfiguration(), metaInfo.getTypeSerializerConfigSnapshot(StateMetaInfo.CommonSerializerKeys.NAMESPACE_SERIALIZER));
		Assert.assertEquals(stateSerializer.snapshotConfiguration(), metaInfo.getTypeSerializerConfigSnapshot(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER));
	}

	@Test
	public void testOperatorBackendSerializationProxyRoundtrip() throws Exception {

		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;
		TypeSerializer<?> keySerializer = DoubleSerializer.INSTANCE;
		TypeSerializer<?> valueSerializer = StringSerializer.INSTANCE;

		List<StateMetaInfo.Snapshot> stateMetaInfoSnapshots = new ArrayList<>();

		stateMetaInfoSnapshots.add(StateMetaInfo.Builder.forOperatorState(
			"a", stateSerializer, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE).snapshot());
		stateMetaInfoSnapshots.add(StateMetaInfo.Builder.forOperatorState(
			"b", stateSerializer, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE).snapshot());
		stateMetaInfoSnapshots.add(StateMetaInfo.Builder.forOperatorState(
			"c", stateSerializer, OperatorStateHandle.Mode.UNION).snapshot());

		List<StateMetaInfo.Snapshot> broadcastStateMetaInfoSnapshots = new ArrayList<>();

		broadcastStateMetaInfoSnapshots.add(StateMetaInfo.Builder.forBroadcastState(
				"d", keySerializer, valueSerializer).snapshot());
		broadcastStateMetaInfoSnapshots.add(StateMetaInfo.Builder.forBroadcastState(
				"e", valueSerializer, keySerializer).snapshot());

		OperatorBackendSerializationProxy serializationProxy =
				new OperatorBackendSerializationProxy(stateMetaInfoSnapshots, broadcastStateMetaInfoSnapshots);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			serializationProxy.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		serializationProxy =
				new OperatorBackendSerializationProxy(Thread.currentThread().getContextClassLoader());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			serializationProxy.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(stateMetaInfoSnapshots, serializationProxy.getOperatorStateMetaInfoSnapshots());
		Assert.assertEquals(broadcastStateMetaInfoSnapshots, serializationProxy.getBroadcastStateMetaInfoSnapshots());
	}

	@Test
	public void testOperatorStateMetaInfoSerialization() throws Exception {

		String name = "test";
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		StateMetaInfo.Snapshot metaInfo =
			StateMetaInfo.Builder.forOperatorState(
				name, stateSerializer, OperatorStateHandle.Mode.UNION).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			StateMetaInfoSnapshotReadersWriters.getWriter().writeStateMetaInfoSnapshot(metaInfo, new DataOutputViewStreamWrapper(out));

			serialized = out.toByteArray();
		}

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			final StateMetaInfoReader reader = StateMetaInfoSnapshotReadersWriters.getReader(
				CURRENT_STATE_META_INFO_SNAPSHOT_VERSION, StateMetaInfoSnapshotReadersWriters.StateTypeHint.OPERATOR_STATE);
			metaInfo = reader.readStateMetaInfoSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		Assert.assertEquals(name, metaInfo.getName());
		Assert.assertEquals(
			OperatorStateHandle.Mode.UNION,
			OperatorStateHandle.Mode.valueOf(metaInfo.getOption(StateMetaInfoBase.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE)));
		Assert.assertEquals(stateSerializer, metaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER));
	}

	@Test
	public void testBroadcastStateMetaInfoSerialization() throws Exception {

		String name = "test";
		TypeSerializer<?> keySerializer = DoubleSerializer.INSTANCE;
		TypeSerializer<?> valueSerializer = StringSerializer.INSTANCE;

		StateMetaInfo.Snapshot metaInfo =
			StateMetaInfo.Builder.forBroadcastState(
						name, keySerializer, valueSerializer).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			StateMetaInfoSnapshotReadersWriters.getWriter().
				writeStateMetaInfoSnapshot(metaInfo, new DataOutputViewStreamWrapper(out));

			serialized = out.toByteArray();
		}

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			final StateMetaInfoReader reader = StateMetaInfoSnapshotReadersWriters.getReader(
				CURRENT_STATE_META_INFO_SNAPSHOT_VERSION, StateMetaInfoSnapshotReadersWriters.StateTypeHint.OPERATOR_STATE);
			metaInfo = reader.readStateMetaInfoSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		Assert.assertEquals(name, metaInfo.getName());
		Assert.assertEquals(
			OperatorStateHandle.Mode.BROADCAST,
			OperatorStateHandle.Mode.valueOf(metaInfo.getOption(StateMetaInfoBase.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE)));
		Assert.assertEquals(keySerializer, metaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.KEY_SERIALIZER));
		Assert.assertEquals(valueSerializer, metaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER));
	}

	@Test
	public void testOperatorStateMetaInfoReadSerializerFailureResilience() throws Exception {
		String name = "test";
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		StateMetaInfo.Snapshot metaInfo =
			StateMetaInfo.Builder.forOperatorState(
				name, stateSerializer, OperatorStateHandle.Mode.UNION).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			StateMetaInfoSnapshotReadersWriters.getWriter().
				writeStateMetaInfoSnapshot(metaInfo, new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		Set<String> cnfThrowingSerializerClasses = new HashSet<>();
		cnfThrowingSerializerClasses.add(DoubleSerializer.class.getName());
		cnfThrowingSerializerClasses.add(StringSerializer.class.getName());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			final StateMetaInfoReader reader = StateMetaInfoSnapshotReadersWriters.getReader(
				CURRENT_STATE_META_INFO_SNAPSHOT_VERSION, StateMetaInfoSnapshotReadersWriters.StateTypeHint.OPERATOR_STATE);
			final ClassLoader classLoader = new ArtificialCNFExceptionThrowingClassLoader(
				Thread.currentThread().getContextClassLoader(),
				cnfThrowingSerializerClasses);
			metaInfo = reader.readStateMetaInfoSnapshot(new DataInputViewStreamWrapper(in), classLoader);
		}

		Assert.assertEquals(name, metaInfo.getName());
		Assert.assertTrue(metaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER) instanceof UnloadableDummyTypeSerializer);
		Assert.assertEquals(
			stateSerializer.snapshotConfiguration(),
			metaInfo.getTypeSerializerConfigSnapshot(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER));
	}

	@Test
	public void testBroadcastStateMetaInfoReadSerializerFailureResilience() throws Exception {
		String broadcastName = "broadcastTest";
		TypeSerializer<?> keySerializer = DoubleSerializer.INSTANCE;
		TypeSerializer<?> valueSerializer = StringSerializer.INSTANCE;

		StateMetaInfo.Snapshot broadcastMetaInfo =
			StateMetaInfo.Builder.forBroadcastState(
						broadcastName, keySerializer, valueSerializer).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			StateMetaInfoSnapshotReadersWriters.getWriter().
				writeStateMetaInfoSnapshot(broadcastMetaInfo, new DataOutputViewStreamWrapper(out));

			serialized = out.toByteArray();
		}

		Set<String> cnfThrowingSerializerClasses = new HashSet<>();
		cnfThrowingSerializerClasses.add(DoubleSerializer.class.getName());
		cnfThrowingSerializerClasses.add(StringSerializer.class.getName());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			final StateMetaInfoReader reader =
				StateMetaInfoSnapshotReadersWriters.getReader(
					CURRENT_STATE_META_INFO_SNAPSHOT_VERSION,
					StateMetaInfoSnapshotReadersWriters.StateTypeHint.OPERATOR_STATE);

			final ClassLoader classLoader = new ArtificialCNFExceptionThrowingClassLoader(
				Thread.currentThread().getContextClassLoader(),
				cnfThrowingSerializerClasses);

			broadcastMetaInfo = reader.readStateMetaInfoSnapshot(new DataInputViewStreamWrapper(in), classLoader);
		}

		Assert.assertEquals(broadcastName, broadcastMetaInfo.getName());
		Assert.assertTrue(broadcastMetaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.KEY_SERIALIZER) instanceof UnloadableDummyTypeSerializer);
		Assert.assertEquals(keySerializer.snapshotConfiguration(), broadcastMetaInfo.getTypeSerializerConfigSnapshot(StateMetaInfo.CommonSerializerKeys.KEY_SERIALIZER));
		Assert.assertTrue(broadcastMetaInfo.getTypeSerializer(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER) instanceof UnloadableDummyTypeSerializer);
		Assert.assertEquals(valueSerializer.snapshotConfiguration(), broadcastMetaInfo.getTypeSerializerConfigSnapshot(StateMetaInfo.CommonSerializerKeys.VALUE_SERIALIZER));
	}

	/**
	 * This test fixes the order of elements in the enum which is important for serialization. Do not modify this test
	 * except if you are entirely sure what you are doing.
	 */
	@Test
	public void testFixTypeOrder() {
		// ensure all elements are covered
		Assert.assertEquals(7, StateDescriptor.Type.values().length);
		// fix the order of elements to keep serialization format stable
		Assert.assertEquals(0, StateDescriptor.Type.UNKNOWN.ordinal());
		Assert.assertEquals(1, StateDescriptor.Type.VALUE.ordinal());
		Assert.assertEquals(2, StateDescriptor.Type.LIST.ordinal());
		Assert.assertEquals(3, StateDescriptor.Type.REDUCING.ordinal());
		Assert.assertEquals(4, StateDescriptor.Type.FOLDING.ordinal());
		Assert.assertEquals(5, StateDescriptor.Type.AGGREGATING.ordinal());
		Assert.assertEquals(6, StateDescriptor.Type.MAP.ordinal());
	}
}
