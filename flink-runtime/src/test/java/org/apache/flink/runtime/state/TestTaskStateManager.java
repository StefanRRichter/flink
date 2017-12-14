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
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Implementation of {@link TaskStateManager} for tests.
 */
public class TestTaskStateManager implements TaskStateManager {

	private long reportedCheckpointId;

	private JobID jobId;
	private ExecutionAttemptID executionAttemptID;

	private final Map<Long, TaskStateSnapshot> jobManagerTaskStateSnapshotsByCheckpointId;
	private final Map<Long, TaskStateSnapshot> taskManagerTaskStateSnapshotsByCheckpointId;
	private CheckpointResponder checkpointResponder;
	private OneShotLatch waitForReportLatch;
	private LocalRecoveryDirectoryProvider localRecoveryDirectoryProvider;

	public TestTaskStateManager() {
		this(new JobID(), new ExecutionAttemptID(), new TestCheckpointResponder());
	}

	public TestTaskStateManager(
		JobID jobId,
		ExecutionAttemptID executionAttemptID) {

		this(jobId, executionAttemptID, null);
	}

	public TestTaskStateManager(
		JobID jobId,
		ExecutionAttemptID executionAttemptID,
		CheckpointResponder checkpointResponder) {
		this(jobId, executionAttemptID, checkpointResponder, createTestLocalRecoveryDirectoryProvider());
	}

	public TestTaskStateManager(
		JobID jobId,
		ExecutionAttemptID executionAttemptID,
		CheckpointResponder checkpointResponder,
		LocalRecoveryDirectoryProvider localRecoveryDirectoryProvider) {
		this.jobId = jobId;
		this.executionAttemptID = executionAttemptID;
		this.checkpointResponder = checkpointResponder;
		this.localRecoveryDirectoryProvider = localRecoveryDirectoryProvider;
		this.jobManagerTaskStateSnapshotsByCheckpointId = new HashMap<>();
		this.taskManagerTaskStateSnapshotsByCheckpointId = new HashMap<>();
		this.reportedCheckpointId = -1L;
	}

	@Override
	public void reportTaskStateSnapshots(
		@Nonnull CheckpointMetaData checkpointMetaData,
		@Nonnull CheckpointMetrics checkpointMetrics,
		@Nullable TaskStateSnapshot acknowledgedState,
		@Nullable TaskStateSnapshot localState) {


		jobManagerTaskStateSnapshotsByCheckpointId.put(
			checkpointMetaData.getCheckpointId(),
			acknowledgedState);

		taskManagerTaskStateSnapshotsByCheckpointId.put(
			checkpointMetaData.getCheckpointId(),
			localState);

		if (checkpointResponder != null) {
			checkpointResponder.acknowledgeCheckpoint(
				jobId,
				executionAttemptID,
				checkpointMetaData.getCheckpointId(),
				checkpointMetrics,
				acknowledgedState);
		}

		this.reportedCheckpointId = checkpointMetaData.getCheckpointId();

		if (waitForReportLatch != null) {
			waitForReportLatch.trigger();
		}
	}

	@Nonnull
	@Override
	public PrioritizedOperatorSubtaskState prioritizedOperatorState(OperatorID operatorID) {
		TaskStateSnapshot jmTaskStateSnapshot = getLastJobManagerTaskStateSnapshot();
		TaskStateSnapshot tmTaskStateSnapshot = getLastTaskManagerTaskStateSnapshot();

		OperatorSubtaskState jmOpState = null;
		List<OperatorSubtaskState> tmStateCollection = null;

		if (jmTaskStateSnapshot != null) {
			jmOpState = jmTaskStateSnapshot.getSubtaskStateByOperatorID(operatorID);
		}

		if (tmTaskStateSnapshot != null) {
			OperatorSubtaskState tmOpState = tmTaskStateSnapshot.getSubtaskStateByOperatorID(operatorID);
			if (tmOpState != null) {
				tmStateCollection = Collections.singletonList(tmOpState);
			}
		}

		if (jmOpState == null) {
			jmOpState = new OperatorSubtaskState();
		}

		if (tmStateCollection == null) {
			tmStateCollection = Collections.emptyList();
		}

		return new PrioritizedOperatorSubtaskState(jmOpState, tmStateCollection);
	}

	@Nonnull
	@Override
	public LocalRecoveryDirectoryProvider createLocalRecoveryRootDirectoryProvider() {
		return Preconditions.checkNotNull(localRecoveryDirectoryProvider,
			"Local state directory was never set for this test object!");
	}

	public void setLocalRecoveryDirectoryProvider(LocalRecoveryDirectoryProvider recoveryDirectoryProvider) {
		this.localRecoveryDirectoryProvider = recoveryDirectoryProvider;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {

	}

	public JobID getJobId() {
		return jobId;
	}

	public void setJobId(JobID jobId) {
		this.jobId = jobId;
	}

	public ExecutionAttemptID getExecutionAttemptID() {
		return executionAttemptID;
	}

	public void setExecutionAttemptID(ExecutionAttemptID executionAttemptID) {
		this.executionAttemptID = executionAttemptID;
	}

	public CheckpointResponder getCheckpointResponder() {
		return checkpointResponder;
	}

	public void setCheckpointResponder(CheckpointResponder checkpointResponder) {
		this.checkpointResponder = checkpointResponder;
	}

	public Map<Long, TaskStateSnapshot> getJobManagerTaskStateSnapshotsByCheckpointId() {
		return jobManagerTaskStateSnapshotsByCheckpointId;
	}

	public void setJobManagerTaskStateSnapshotsByCheckpointId(
		Map<Long, TaskStateSnapshot> jobManagerTaskStateSnapshotsByCheckpointId) {
		this.jobManagerTaskStateSnapshotsByCheckpointId.clear();
		this.jobManagerTaskStateSnapshotsByCheckpointId.putAll(jobManagerTaskStateSnapshotsByCheckpointId);
	}

	public Map<Long, TaskStateSnapshot> getTaskManagerTaskStateSnapshotsByCheckpointId() {
		return taskManagerTaskStateSnapshotsByCheckpointId;
	}

	public void setTaskManagerTaskStateSnapshotsByCheckpointId(
		Map<Long, TaskStateSnapshot> taskManagerTaskStateSnapshotsByCheckpointId) {
		this.taskManagerTaskStateSnapshotsByCheckpointId.clear();
		this.taskManagerTaskStateSnapshotsByCheckpointId.putAll(taskManagerTaskStateSnapshotsByCheckpointId);
	}

	public long getReportedCheckpointId() {
		return reportedCheckpointId;
	}

	public void setReportedCheckpointId(long reportedCheckpointId) {
		this.reportedCheckpointId = reportedCheckpointId;
	}

	public TaskStateSnapshot getLastJobManagerTaskStateSnapshot() {
		return jobManagerTaskStateSnapshotsByCheckpointId != null ?
			jobManagerTaskStateSnapshotsByCheckpointId.get(reportedCheckpointId)
			: null;
	}

	public TaskStateSnapshot getLastTaskManagerTaskStateSnapshot() {
		return taskManagerTaskStateSnapshotsByCheckpointId != null ?
			taskManagerTaskStateSnapshotsByCheckpointId.get(reportedCheckpointId)
			: null;
	}

	public OneShotLatch getWaitForReportLatch() {
		return waitForReportLatch;
	}

	public void setWaitForReportLatch(OneShotLatch waitForReportLatch) {
		this.waitForReportLatch = waitForReportLatch;
	}

	public void restoreLatestCheckpointState(Map<Long, TaskStateSnapshot> taskStateSnapshotsByCheckpointId) {

		if (taskStateSnapshotsByCheckpointId == null
			|| taskStateSnapshotsByCheckpointId.isEmpty()) {
			return;
		}

		long latestId = -1;

		for (long id : taskStateSnapshotsByCheckpointId.keySet()) {
			if (id > latestId) {
				latestId = id;
			}
		}

		setReportedCheckpointId(latestId);
		setJobManagerTaskStateSnapshotsByCheckpointId(taskStateSnapshotsByCheckpointId);
	}

	private static LocalRecoveryDirectoryProvider createTestLocalRecoveryDirectoryProvider() {
		File rootDir = new File(System.getProperty("java.io.tmpdir"), "testLocalState_" + UUID.randomUUID());
		if (!rootDir.exists()) {
			Preconditions.checkState(rootDir.mkdirs());
		}
		return new LocalRecoveryDirectoryProvider(rootDir, UUID.randomUUID().toString());
	}
}
