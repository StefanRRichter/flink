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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * This class holds the all {@link TaskLocalStateStoreImpl} objects for a task executor (manager).
 */
public class TaskExecutorLocalStateStoresManager {

	/** Logger for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorLocalStateStoresManager.class);

	/**
	 * This map holds all local state stores for tasks running on the task manager / executor that own the instance of
	 * this. Maps from allocation id to all the subtask's local state stores.
	 */
	@GuardedBy("lock")
	private final Map<AllocationID, Map<JobVertexSubtaskKey, TaskLocalStateStore>> taskStateStoresByAllocationID;

	/** The configured mode for local recovery on this task manager. */
	private final LocalRecoveryConfig.LocalRecoveryMode localRecoveryMode;

	/** This is the root directory for all local state of this task manager / executor. */
	private final File[] localStateRootDirectories;

	/** Executor that runs the discarding of released state objects. */
	private final Executor discardExecutor;

	/** Guarding lock for taskStateStoresByAllocationID and closed-flag. */
	private final Object lock;

	@GuardedBy("lock")
	private boolean closed;

	public TaskExecutorLocalStateStoresManager(
		@Nonnull LocalRecoveryConfig.LocalRecoveryMode localRecoveryMode,
		@Nonnull File[] localStateRootDirectories,
		@Nonnull Executor discardExecutor) {

		this.taskStateStoresByAllocationID = new HashMap<>();
		this.localRecoveryMode = localRecoveryMode;
		this.localStateRootDirectories = localStateRootDirectories;
		this.discardExecutor = discardExecutor;
		this.lock = new Object();
		this.closed = false;

		for (File localStateRecoveryRootDir : localStateRootDirectories) {
			if (!localStateRecoveryRootDir.exists()) {

				if (!localStateRecoveryRootDir.mkdirs()) {
					throw new IllegalStateException("Could not create root directory for local recovery: " +
						localStateRecoveryRootDir);
				}
			}
		}
	}

	@Nonnull
	public TaskLocalStateStore localStateStoreForSubtask(
		@Nonnull JobID jobId,
		@Nonnull AllocationID allocationID,
		@Nonnull JobVertexID jobVertexID,
		@Nonnegative int subtaskIndex) {

		synchronized (lock) {

			if (closed) {
				throw new IllegalStateException("TaskExecutorLocalStateStoresManager is already closed and cannot " +
					"register a new TaskLocalStateStore.");
			}

			final Map<JobVertexSubtaskKey, TaskLocalStateStore> taskStateManagers =
				this.taskStateStoresByAllocationID.computeIfAbsent(allocationID, k -> new HashMap<>());

			final JobVertexSubtaskKey taskKey = new JobVertexSubtaskKey(jobVertexID, subtaskIndex);

			LocalRecoveryDirectoryProviderImpl directoryProvider = new LocalRecoveryDirectoryProviderImpl(
				localStateRootDirectories,
				jobId,
				allocationID,
				jobVertexID,
				subtaskIndex);

			LocalRecoveryConfig localRecoveryConfig = new LocalRecoveryConfig(
				localRecoveryMode,
				directoryProvider);

			return taskStateManagers.computeIfAbsent(
				taskKey,
				k -> new TaskLocalStateStoreImpl(
					jobId,
					allocationID,
					jobVertexID,
					subtaskIndex,
					localRecoveryConfig,
					discardExecutor));
		}
	}

	public void releaseLocalStateForAllocationId(@Nonnull AllocationID allocationID) {

		Map<JobVertexSubtaskKey, TaskLocalStateStore> cleanupLocalStores;

		synchronized (lock) {
			if (closed) {
				return;
			}
			cleanupLocalStores = taskStateStoresByAllocationID.remove(allocationID);
		}

		if (cleanupLocalStores != null) {
			doRelease(cleanupLocalStores.values());
		}
	}

	public void shutdown() {

		ArrayList<Map<JobVertexSubtaskKey, TaskLocalStateStore>> toRelease;

		synchronized (lock) {
			closed = true;
			toRelease = new ArrayList<>(taskStateStoresByAllocationID.values());
			taskStateStoresByAllocationID.clear();
		}

		for (Map<JobVertexSubtaskKey, TaskLocalStateStore> stateStoreMap : toRelease) {
			doRelease(stateStoreMap.values());
		}
	}

	private void doRelease(Iterable<TaskLocalStateStore> toRelease) {

		if (toRelease != null) {

			for (TaskLocalStateStore stateStore : toRelease) {
				try {
					stateStore.dispose();
				} catch (Exception disposeEx) {
					LOG.warn("Exception while disposing local state store", disposeEx);
				}
			}
		}
	}

	@VisibleForTesting
	File[] getLocalStateRootDirectories() {
		return localStateRootDirectories;
	}

	@VisibleForTesting
	public LocalRecoveryConfig.LocalRecoveryMode getLocalRecoveryMode() {
		return localRecoveryMode;
	}

	/**
	 * Composite key of {@link JobVertexID} and subtask index that describes the subtask of a job vertex.
	 */
	private static final class JobVertexSubtaskKey {

		/** The job vertex id. */
		@Nonnull
		final JobVertexID jobVertexID;

		/** The subtask index. */
		@Nonnegative
		final int subtaskIndex;

		JobVertexSubtaskKey(@Nonnull JobVertexID jobVertexID, @Nonnegative int subtaskIndex) {
			this.jobVertexID = jobVertexID;
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

			return subtaskIndex == that.subtaskIndex && jobVertexID.equals(that.jobVertexID);
		}

		@Override
		public int hashCode() {
			int result = jobVertexID.hashCode();
			result = 31 * result + subtaskIndex;
			return result;
		}
	}
}
