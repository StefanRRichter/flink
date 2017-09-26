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
 * This class is the default implementation of {@link TaskStateManager} and collaborates with the job manager
 * through {@link CheckpointResponder}) as well as a task-manager-local state store. Like this, client code does
 * not have to deal with the differences between remote or local state on recovery because this class handles both
 * cases transparently.
 *
 * Reported state is tagged by clients so that this class can properly forward to the right receiver for the
 * checkpointed state.
 *
 * TODO: all interaction with local state store must still be implemented! It is currently just a placeholder.
 */
public class TaskStateManagerImpl implements TaskStateManager {

	/** The id of the job for which this manager was created, can report, and recover. */
	private final JobID jobId;

	/** The execution attempt id that this manager reports for. */
	private final ExecutionAttemptID executionAttemptID;

	/** The data given by the job manager to restore the job. This is not set for a new job without previous state. */
	private final TaskRestore jobManagerTaskRestore;

	/** The local state store to which this manager reports local state snapshots. */
	private final LocalStateStore localStateStore;

	/** The checkpoint responder through which this manager can report to the job manager. */
	private final CheckpointResponder checkpointResponder;

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

		/*
		TODO!!!!!!!
		1) lookup local states for a matching operatorID / checkpointID.
		2) if nothing available: look into job manager provided state.
		3) massage it into a snapshots and return stuff.
		 */
	}

	/**
	 * Tracking when local state can be disposed.
	 */
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		//TODO activate and prune local state
	}
}
