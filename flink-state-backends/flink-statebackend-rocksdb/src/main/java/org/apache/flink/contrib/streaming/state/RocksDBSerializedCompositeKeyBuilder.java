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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;

/**
 * @param <K> type of the key.
 */
@NotThreadSafe
class RocksDBSerializedCompositeKeyBuilder<K> {

	@Nonnull
	private final TypeSerializer<K> keySerializer;
	@Nonnull
	private final ByteArrayOutputStreamWithPos keyOutStream;
	@Nonnull
	private final DataOutputView keyOutView;
	@Nonnegative
	private final int keyGroupPrefixBytes;

	private final boolean keySerializerTypeVariableSized;

	@Nonnegative
	private int keyMark;

	RocksDBSerializedCompositeKeyBuilder(
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnegative int keyGroupPrefixBytes,
		@Nonnegative int initialSize) {

		this.keySerializer = keySerializer;
		this.keyOutStream = new ByteArrayOutputStreamWithPos(initialSize);
		this.keyOutView = new DataOutputViewStreamWrapper(keyOutStream);
		this.keyGroupPrefixBytes = keyGroupPrefixBytes;
		this.keySerializerTypeVariableSized = RocksDBKeySerializationUtils.isSerializerTypeVariableSized(keySerializer);
		this.keyMark = 0;
	}

	void setKeyAndKeyGroup(@Nonnull K key, @Nonnegative int keyGroupId) {
		try {
			serializeKeyGroupAndKey(key, keyGroupId);
		} catch (IOException shouldNeverHappen) {
			throw new FlinkRuntimeException(shouldNeverHappen);
		}
	}

	<N> byte[] buildCompositeKeyNamespace(@Nonnull N namespace, @Nonnull TypeSerializer<N> namespaceSerializer) {
		try {
			serializeNamespace(namespace, namespaceSerializer);
			final byte[] result = keyOutStream.toByteArray();
			resetToKey();
			return result;
		} catch (IOException shouldNeverHappen) {
			throw new FlinkRuntimeException(shouldNeverHappen);
		}
	}

	<N, UK> byte[] buildCompositeKeyNamesSpaceUserKey(
		@Nonnull N namespace,
		@Nonnull TypeSerializer<N> namespaceSerializer,
		@Nonnull UK userKey,
		@Nonnull TypeSerializer<UK> userKeySerializer) throws IOException {

		serializeNamespace(namespace, namespaceSerializer);
		userKeySerializer.serialize(userKey, keyOutView);
		byte[] result = keyOutStream.toByteArray();
		resetToKey();
		return result;
	}

	private void serializeKeyGroupAndKey(K key, int keyGroupId) throws IOException {

		resetFully();

		// write key-group
		RocksDBKeySerializationUtils.writeKeyGroup(
			keyGroupId,
			keyGroupPrefixBytes,
			keyOutView);

		// write key
		keySerializer.serialize(key, keyOutView);
		keyMark = keyOutStream.getPosition();
	}

	private <N> void serializeNamespace(
		@Nonnull N namespace,
		@Nonnull TypeSerializer<N> namespaceSerializer) throws IOException {

		assert isKeyWritten();

		final boolean ambiguousKeyPossible =
			keySerializerTypeVariableSized &
				RocksDBKeySerializationUtils.isSerializerTypeVariableSized(namespaceSerializer);

		if (ambiguousKeyPossible) {
			RocksDBKeySerializationUtils.writeVariableIntBytes(
				keyMark - keyGroupPrefixBytes,
				keyOutView);
		}

		RocksDBKeySerializationUtils.writeNameSpace(
			namespace,
			namespaceSerializer,
			keyOutStream,
			keyOutView,
			ambiguousKeyPossible);
	}

	private void resetFully() {
		keyMark = 0;
		keyOutStream.setPosition(0);
	}

	private void resetToKey() {
		this.keyOutStream.setPosition(keyMark);
	}

	private boolean isKeyWritten() {
		return keyMark > 0;
	}
}
