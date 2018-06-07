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
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.IOException;

/**
 * TODO
 */
public class RocksValueDeserializer {

	private final DataInputViewStreamWrapper inView;
	private final ByteArrayInputStreamWithPos inStream;

	private final DataOutputViewStreamWrapper outView;
	private final ByteArrayOutputStreamWithPos outStream;

	public RocksValueDeserializer() {
		this.inStream = new ByteArrayInputStreamWithPos();
		this.inView = new DataInputViewStreamWrapper(inStream);
		this.outStream = new ByteArrayOutputStreamWithPos();
		this.outView = new DataOutputViewStreamWrapper(outStream);
	}


	<V> byte[] serializeValue(V value, TypeSerializer<V> valueSerializer) throws IOException {
		valueSerializer.serialize(value, outView);
		final byte[] result = outStream.toByteArray();
		outStream.reset();
		return result;
	}

	<V> V deserializeValue(byte[] data, TypeSerializer<V> valueSerializer) throws IOException {
		return deserializeValue(data, 0, data.length, valueSerializer);
	}

	<V> V deserializeValue(byte[] data, int off, int len, TypeSerializer<V> valueSerializer) throws IOException {
		inStream.setInternalArray(data, off, len);
		return valueSerializer.deserialize(inView);
	}
}
