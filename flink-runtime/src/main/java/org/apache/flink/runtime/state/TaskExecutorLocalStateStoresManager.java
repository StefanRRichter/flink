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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.Map;

public class TaskExecutorLocalStateStoresManager {

	private final Map<JobID, Map<JobVertexSubtaskKey, LocalStateStore>> taskStateManagers;

	public TaskExecutorLocalStateStoresManager() {
		this.taskStateManagers = new HashMap<>();
	}

	public LocalStateStore localStateStoreForTask(
		JobID jobId,
		JobVertexID jobVertexID,
		int subtaskIndex) {

		Preconditions.checkNotNull(jobId);
		final JobVertexSubtaskKey taskKey = new JobVertexSubtaskKey(jobVertexID, subtaskIndex);

		final Map<JobVertexSubtaskKey, LocalStateStore> taskStateManagers =
			this.taskStateManagers.computeIfAbsent(jobId, k -> new HashMap<>());

		return taskStateManagers.computeIfAbsent(
			taskKey, k -> new LocalStateStore(jobId, jobVertexID, subtaskIndex));
	}

	public void releaseJob(JobID jobID) {

		Map<JobVertexSubtaskKey, LocalStateStore> cleanupLocalStores = taskStateManagers.remove(jobID);

		if (cleanupLocalStores != null) {
			doRelease(cleanupLocalStores.values());
		}
	}

	public void releaseAll() {

		for (Map<JobVertexSubtaskKey, LocalStateStore> stateStoreMap : taskStateManagers.values()) {
			doRelease(stateStoreMap.values());
		}

		taskStateManagers.clear();
	}

	private void doRelease(Iterable<LocalStateStore> toRelease) {
		if (toRelease != null) {
			for (LocalStateStore stateStore : toRelease) {
				stateStore.dispose();
			}
		}
	}

	private static final class JobVertexSubtaskKey {

		@Nonnull
		final JobVertexID jobVertexID;
		final int subtaskIndex;

		public JobVertexSubtaskKey(@Nonnull JobVertexID jobVertexID, int subtaskIndex) {
			this.jobVertexID = Preconditions.checkNotNull(jobVertexID);
			this.subtaskIndex = subtaskIndex;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			JobVertexSubtaskKey that = (JobVertexSubtaskKey) o;

			if (subtaskIndex != that.subtaskIndex) {
				return false;
			}
			return jobVertexID.equals(that.jobVertexID);
		}

		@Override
		public int hashCode() {
			int result = jobVertexID.hashCode();
			result = 31 * result + subtaskIndex;
			return result;
		}
	}
}
