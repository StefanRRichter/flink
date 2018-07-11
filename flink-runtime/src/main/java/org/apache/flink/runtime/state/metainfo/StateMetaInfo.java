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

package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.runtime.state.OperatorStateHandle;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * General class to represent meta information about one state in a state backend.
 */
public class StateMetaInfo extends StateMetaInfoBase {

	/**
	 * Predefined keys for the most common serializer types in the meta info.
	 */
	public enum CommonSerializerKeys {
		KEY_SERIALIZER,
		NAMESPACE_SERIALIZER,
		VALUE_SERIALIZER
	}

	/** Map from a string key to type serializers of one state in a backend. */
	@Nonnull
	private final Map<String, TypeSerializer<?>> serializers;

	public StateMetaInfo(@Nonnull String name) {
		this(name, new HashMap<>(4));
	}

	public StateMetaInfo(
		@Nonnull String name,
		@Nonnull Map<String, String> options) {
		this(name, options, new HashMap<>(4));
	}

	public StateMetaInfo(
		@Nonnull String name,
		@Nonnull Map<String, String> options,
		@Nonnull Map<String, TypeSerializer<?>> serializers) {
		super(name, options);
		this.serializers = serializers;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		StateMetaInfo that = (StateMetaInfo) o;
		return name.equals(that.name) &&
			options.equals(that.options) &&
			serializers.equals(that.serializers);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, options, serializers);
	}

	@Nullable
	public TypeSerializer<?> getTypeSerializer(@Nonnull String key) {
		return serializers.get(key);
	}

	public void setTypeSerializer(@Nonnull String key, @Nullable TypeSerializer<?> typeSerializer) {
		if (typeSerializer != null) {
			serializers.put(key, typeSerializer);
		} else {
			serializers.remove(key);
		}
	}

	@Nullable
	public TypeSerializer<?> getTypeSerializer(@Nonnull CommonSerializerKeys key) {
		return getTypeSerializer(key.toString());
	}

	public void setTypeSerializer(@Nonnull CommonSerializerKeys key, @Nullable TypeSerializer<?> typeSerializer) {
		setTypeSerializer(key.toString(), typeSerializer);
	}

	@Nonnull
	public Map<String, TypeSerializer<?>> getSerializersImmutable() {
		return Collections.unmodifiableMap(serializers);
	}

	public Snapshot snapshot() {
		final Map<String, String> optionsCopy = new HashMap<>(options);
		final Map<String, TypeSerializer<?>> serializerDeepCopy = new HashMap<>(serializers.size());
		final Map<String, TypeSerializerConfigSnapshot> serializerConfigSnapshots = new HashMap<>(serializers.size());
		for (Map.Entry<String, TypeSerializer<?>> entry : serializers.entrySet()) {
			final TypeSerializer<?> serializer = entry.getValue();
			if (serializer != null) {
				final String key = entry.getKey();
				serializerDeepCopy.put(key, serializer.duplicate());
				serializerConfigSnapshots.put(key, serializer.snapshotConfiguration());
			}
		}
		return new Snapshot(
			name,
			optionsCopy,
			serializerConfigSnapshots,
			serializerDeepCopy);
	}

	public StateMetaInfo deepCopy() {
		return new StateMetaInfo(
			name,
			new HashMap<>(options),
			serializers.entrySet()
				.stream()
				.filter(entry -> entry.getValue() != null)
				.collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().duplicate())));
	}

	/**
	 * Snapshot of a {@link StateMetaInfo} to write the information as part of a checkpoint.
	 */
	public static class Snapshot extends StateMetaInfoBase {

		/** The configurations of all the type serializers used with the state. */
		@Nonnull
		private final Map<String, TypeSerializerConfigSnapshot> serializerConfigSnapshots;

		// TODO this will go awy again after FLINK-9377 is merged, that is why it is currently duplicated here.
		@Nonnull
		private final Map<String, TypeSerializer<?>> serializers;

		public Snapshot(@Nonnull String name) {
			this(name, new HashMap<>(4), new HashMap<>(4));
		}

		public Snapshot(
			@Nonnull String name,
			@Nonnull Map<String, String> options,
			@Nonnull Map<String, TypeSerializerConfigSnapshot> serializerConfigSnapshots) {
			this(name, options, serializerConfigSnapshots, new HashMap<>(4));
		}

		public Snapshot(
			@Nonnull String name,
			@Nonnull Map<String, String> options,
			@Nonnull Map<String, TypeSerializerConfigSnapshot> serializerConfigSnapshots,
			@Nonnull Map<String, TypeSerializer<?>> serializers) {
			super(name, options);
			this.serializerConfigSnapshots = serializerConfigSnapshots;
			this.serializers = serializers;
		}

		@Nullable
		public TypeSerializerConfigSnapshot getTypeSerializerConfigSnapshot(@Nonnull String key) {
			return serializerConfigSnapshots.get(key);
		}

		public void setTypeSerializerConfigSnapshot(
			@Nonnull String key,
			@Nullable TypeSerializerConfigSnapshot typeSerializer) {

			if (typeSerializer != null) {
				serializerConfigSnapshots.put(key, typeSerializer);
			} else {
				serializerConfigSnapshots.remove(key);
			}
		}

		@Nullable
		public TypeSerializerConfigSnapshot getTypeSerializerConfigSnapshot(@Nonnull CommonSerializerKeys key) {
			return getTypeSerializerConfigSnapshot(key.toString());
		}

		public void setTypeSerializerConfigSnapshot(
			@Nonnull CommonSerializerKeys key,
			@Nullable TypeSerializerConfigSnapshot typeSerializer) {
			setTypeSerializerConfigSnapshot(key.toString(), typeSerializer);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Snapshot that = (Snapshot) o;
			return name.equals(that.name) &&
				options.equals(that.options) &&
				serializerConfigSnapshots.equals(that.serializerConfigSnapshots);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, options, serializerConfigSnapshots);
		}

		public StateMetaInfo toStateMetaInfo() {
			return new StateMetaInfo(
				name,
				new HashMap<>(options),
				serializers.entrySet()
					.stream()
					.filter(entry -> entry.getValue() != null)
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
		}

		@Nullable
		public TypeSerializer<?> getTypeSerializer(@Nonnull String key) {
			return serializers.get(key);
		}

		public void setTypeSerializer(@Nonnull String key, @Nullable TypeSerializer<?> typeSerializer) {
			if (typeSerializer != null) {
				serializers.put(key, typeSerializer);
			} else {
				serializers.remove(key);
			}
		}

		@Nullable
		public TypeSerializer<?> getTypeSerializer(@Nonnull CommonSerializerKeys key) {
			return getTypeSerializer(key.toString());
		}

		public void setTypeSerializer(@Nonnull CommonSerializerKeys key, @Nullable TypeSerializer<?> typeSerializer) {
			setTypeSerializer(key.toString(), typeSerializer);
		}

		@Nonnull
		public Map<String, TypeSerializerConfigSnapshot> getSerializerConfigSnapshotsImmutable() {
			return Collections.unmodifiableMap(serializerConfigSnapshots);
		}

		@Nonnull
		public Map<String, TypeSerializer<?>> getSerializersImmutable() {
			return Collections.unmodifiableMap(serializers);
		}
	}

	/**
	 * A collection of static methods to simplify creation of {@link StateMetaInfo} for common cases.
	 */
	public static class Builder {
		public static StateMetaInfo forKeyedState(
			@Nonnull String name,
			@Nonnull StateDescriptor.Type stateType,
			@Nonnull TypeSerializer<?> namespaceSerializer,
			@Nonnull TypeSerializer<?> stateSerializer) {

			StateMetaInfo stateMetaInfo = new StateMetaInfo(name);
			stateMetaInfo.setOption(CommonOptionsKeys.KEYED_STATE_TYPE, stateType.toString());
			stateMetaInfo.setTypeSerializer(CommonSerializerKeys.NAMESPACE_SERIALIZER, namespaceSerializer);
			stateMetaInfo.setTypeSerializer(CommonSerializerKeys.VALUE_SERIALIZER, stateSerializer);
			return stateMetaInfo;
		}

		public static StateMetaInfo forOperatorState(
			String name,
			TypeSerializer<?> partitionStateSerializer,
			OperatorStateHandle.Mode assignmentMode) {

			StateMetaInfo stateMetaInfo = new StateMetaInfo(name);
			stateMetaInfo.setOption(CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE, assignmentMode.toString());
			stateMetaInfo.setTypeSerializer(CommonSerializerKeys.VALUE_SERIALIZER, partitionStateSerializer);
			return stateMetaInfo;
		}

		public static StateMetaInfo forBroadcastState(
			@Nonnull String name,
			@Nonnull TypeSerializer<?> keySerializer,
			@Nonnull TypeSerializer<?> valueSerializer) {

			StateMetaInfo stateMetaInfo = new StateMetaInfo(name);
			stateMetaInfo.setOption(
				CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE,
				OperatorStateHandle.Mode.BROADCAST.toString());
			stateMetaInfo.setTypeSerializer(CommonSerializerKeys.KEY_SERIALIZER, keySerializer);
			stateMetaInfo.setTypeSerializer(CommonSerializerKeys.VALUE_SERIALIZER, valueSerializer);
			return stateMetaInfo;
		}
	}
}
