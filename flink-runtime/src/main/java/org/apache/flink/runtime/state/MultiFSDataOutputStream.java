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

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.util.LambdaUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MultiFSDataOutputStream extends FSDataOutputStream {

	private final List<? extends FSDataOutputStream> innerStreams;

	public MultiFSDataOutputStream(FSDataOutputStream... streams) {
		this(Arrays.asList(streams));
	}

	public MultiFSDataOutputStream(List<? extends FSDataOutputStream> streamList) {

		innerStreams = Preconditions.checkNotNull(streamList);

		Preconditions.checkState(!streamList.isEmpty());

		for (OutputStream stream : streamList) {
			Preconditions.checkNotNull(stream);
		}

		try {
			getPos();
		} catch (IOException ioex) {
			throw new IllegalStateException("Streams have different initial positions!", ioex);
		}
	}

	@Override
	public void write(int b) throws IOException {
		for (int i = 0; i < innerStreams.size(); ++i) {
			innerStreams.get(i).write(b);
		}
	}

	@Override
	public void write(byte[] b) throws IOException {
		for (int i = 0; i < innerStreams.size(); ++i) {
			innerStreams.get(i).write(b);
		}
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		for (int i = 0; i < innerStreams.size(); ++i) {
			innerStreams.get(i).write(b, off, len);
		}
	}

	@Override
	public long getPos() throws IOException {

		final long referencePos = innerStreams.get(0).getPos();

		for (int i = 1; i < innerStreams.size(); ++i) {
			long cmpPos = innerStreams.get(i).getPos();
			if (referencePos != cmpPos) {
				throw new IOException("Stream positions are out of sync between stream at index 0 and stream at index "
					+ i + ". Different positions are " + referencePos + " and " + cmpPos + ".");
			}
		}

		return referencePos;
	}

	public List<FSDataOutputStream> getInnerStreams() {
		return Collections.unmodifiableList(innerStreams);
	}

	@Override
	public void flush() throws IOException {
		try {
			LambdaUtil.applyToAllWhileSuppressingExceptions(innerStreams, OutputStream::flush);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void sync() throws IOException {
		try {
			LambdaUtil.applyToAllWhileSuppressingExceptions(innerStreams, OutputStream::flush);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void close() throws IOException {
		try {
			LambdaUtil.applyToAllWhileSuppressingExceptions(innerStreams, OutputStream::close);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
