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
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskRestore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.snapshot.KeyedStateSnapshot;
import org.apache.flink.runtime.state.snapshot.OperatorSubtaskStateReport;
import org.apache.flink.runtime.state.snapshot.SnapshotMetaData;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages the state for a task.
 */
public class TaskStateManagerImpl implements TaskStateManager {

	private final JobID jobId;
	private final TaskRestore jobManagerTaskRestore;
	private final LocalStateStore localStateStore;
	private final ExecutionAttemptID executionAttemptID;
	private final CheckpointResponder checkpointResponder;

//	private final JobVertexID jobVertexID;
//	private final int subtaskIndex;

	public TaskStateManagerImpl(
		JobID jobId,
		LocalStateStore localStateStore,
		TaskRestore jobManagerTaskRestore,
		ExecutionAttemptID executionAttemptID,
		CheckpointResponder checkpointResponder) {

		this.jobId = jobId;
		this.localStateStore = localStateStore;
		this.jobManagerTaskRestore = jobManagerTaskRestore;
		this.executionAttemptID = executionAttemptID;
		this.checkpointResponder = checkpointResponder;
	}

	@Override
	public void reportStateHandles(
		@Nonnull CheckpointMetaData checkpointMetaData,
		@Nonnull CheckpointMetrics checkpointMetrics,
		@Nullable TaskStateSnapshot acknowledgedState) {

		checkpointResponder.acknowledgeCheckpoint(
			jobId,
			executionAttemptID,
			checkpointMetaData.getCheckpointId(),
			checkpointMetrics,
			acknowledgedState);
	}

	/**
	 * Called to report the states of a new operator snapshot.
	 */
	public void reportStateSnapshots(
		@Nonnull CheckpointMetaData checkpointMetaData,
		@Nonnull CheckpointMetrics checkpointMetrics,
		@Nonnull Map<OperatorID, OperatorSubtaskStateReport> subtaskStateReportsByOperator) {

		Preconditions.checkNotNull(subtaskStateReportsByOperator);

		Map<OperatorID, OperatorSubtaskState> subtaskStateByOperatorId =
			new HashMap<>(subtaskStateReportsByOperator.size());

		for (Map.Entry<OperatorID, OperatorSubtaskStateReport> entry : subtaskStateReportsByOperator.entrySet()) {

			OperatorID operatorID = entry.getKey();
			OperatorSubtaskStateReport subtaskStateReport = entry.getValue();

			if (subtaskStateReport != null && subtaskStateReport.hasState()) {

				KeyedStateHandle primaryKeyedBackendStateImage = null;
				Collection<KeyedStateSnapshot> managedKeyedState = subtaskStateReport.getManagedKeyedState();

				if (managedKeyedState != null) {
					for (KeyedStateSnapshot managedKeyedSnapshot : managedKeyedState) {

						if (managedKeyedSnapshot == null) {
							continue;
						}

						if (SnapshotMetaData.Ownership.JobManager.equals(managedKeyedSnapshot.getMetaData().getOwnership())) {
							for (KeyedStateHandle keyedStateHandle : managedKeyedSnapshot) {

								Preconditions.checkState(primaryKeyedBackendStateImage == null,
									"More than one primary state image!");

								primaryKeyedBackendStateImage = keyedStateHandle;
							}
							//TODO store secondary (local) states
						}
					}

					OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(
						subtaskStateReport.getManagedOperatorState(),
						subtaskStateReport.getRawOperatorState(),
						primaryKeyedBackendStateImage,
						subtaskStateReport.getRawKeyedState());

					if (operatorSubtaskState.hasState()) {
						subtaskStateByOperatorId.put(operatorID, operatorSubtaskState);
					}
				}
			}
		}

		TaskStateSnapshot taskStateSnapshot = subtaskStateByOperatorId.isEmpty() ?
			null : new TaskStateSnapshot(subtaskStateByOperatorId);

		checkpointResponder.acknowledgeCheckpoint(
			jobId,
			executionAttemptID,
			checkpointMetaData.getCheckpointId(),
			checkpointMetrics,
			taskStateSnapshot);
	}


	//TODO!!!!! this will later return OperatorSubtaskStateReport
	@Override
	public OperatorSubtaskState operatorStates(OperatorID operatorID) {

		if (jobManagerTaskRestore == null) {
			return null;
		}

		TaskStateSnapshot taskStateSnapshot = jobManagerTaskRestore.getTaskStateSnapshot();
		return taskStateSnapshot.getSubtaskStateByOperatorID(operatorID);

//TODO!!!!!!!
//		/*
//		1) lookup local states for a matching operatorID / checkpointID.
//		2) if nothing available: look into job manager provided state.
//		3) massage it into a snapshots and return stuff.
//		 */
	}

	/**
	 * Tracking when local state can be disposed.
	 */
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		//TODO activate and prune local state
	}

	@Override
	public JobID getJobId() {
		return jobId;
	}
}
