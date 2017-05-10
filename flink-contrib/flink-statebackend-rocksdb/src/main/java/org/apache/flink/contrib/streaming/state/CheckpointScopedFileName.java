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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * This class combines a file name with a checkpoint id and is intended for use as a composite key
 * to avoid races in the registration of shared state, i.e. between the current checkpoint and older
 * checkpoints (that have not been completed at the time the current checkpoint was started) about
 * a file that is shared between them.
 */
public class CheckpointScopedFileName implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Name of a file.
	 */
	private final String fileName;

	/**
	 * Id of the checkpoint that initially created the file.
	 */
	private final long checkpointId;

	public CheckpointScopedFileName(String fileName, long checkpointId) {
		this.fileName = Preconditions.checkNotNull(fileName);
		this.checkpointId = checkpointId;
	}

	public String getFileName() {
		return fileName;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CheckpointScopedFileName that = (CheckpointScopedFileName) o;

		if (getCheckpointId() != that.getCheckpointId()) {
			return false;
		}

		return getFileName().equals(that.getFileName());
	}

	@Override
	public int hashCode() {
		int result = getFileName().hashCode();
		result = 31 * result + (int) (getCheckpointId() ^ (getCheckpointId() >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return checkpointId + "-" + fileName;
	}
}
