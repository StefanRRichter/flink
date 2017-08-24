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
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.image.KeyedBackendStateImage;
import org.apache.flink.runtime.state.image.StateHandlesCollectionKeyedStateImage;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.util.Preconditions;

import com.sun.istack.internal.NotNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages the all checkpointed state for a slot on a TM.
 */
public class SlotStateManager implements CheckpointListener {

	private final JobID jobId;
	private final ExecutionAttemptID executionId;
	private final CheckpointResponder checkpointResponder;

	public SlotStateManager(JobID jobId, ExecutionAttemptID executionId, CheckpointResponder checkpointResponder) {
		this.jobId = jobId;
		this.executionId = executionId;
		this.checkpointResponder = checkpointResponder;
	}

	/**
	 * Called to report the states of a new operator snapshot.
	 */
	public void reportStates(
		@NotNull CheckpointMetaData checkpointMetaData,
		@NotNull CheckpointMetrics checkpointMetrics,
		@NotNull Map<OperatorID, OperatorSubtaskStateReport> subtaskStateReportsByOperator) {

		Preconditions.checkNotNull(subtaskStateReportsByOperator);

		Map<OperatorID, OperatorSubtaskState> subtaskStateByOperatorId =
			new HashMap<>(subtaskStateReportsByOperator.size());

		for (Map.Entry<OperatorID, OperatorSubtaskStateReport> entry : subtaskStateReportsByOperator.entrySet()) {

			OperatorID operatorID = entry.getKey();
			OperatorSubtaskStateReport subtaskStateReport = entry.getValue();

			if (subtaskStateReport != null && subtaskStateReport.hasState()) {


				KeyedStateHandle primaryKeyedBackendStateImage = null;
				Collection<KeyedBackendStateImage> managedKeyedState = subtaskStateReport.getManagedKeyedState();

				if (managedKeyedState != null) {
					for (KeyedBackendStateImage stateImage : managedKeyedState) {

						if (stateImage != null && stateImage.getStateImageMetaData().isPrimary()) {

							Preconditions.checkState(primaryKeyedBackendStateImage == null,
								"More than one primary state image!");

							Preconditions.checkState(stateImage instanceof StateHandlesCollectionKeyedStateImage,
								"Primary image is not based on keyed state handles.");

							Collection<? extends KeyedStateHandle> keyedStateHandles =
								((StateHandlesCollectionKeyedStateImage<?, ? extends KeyedStateHandle>) stateImage).getKeyedStateHandles();

							Preconditions.checkState(keyedStateHandles.size() == 1,
								"Found more than one state handle.");

							primaryKeyedBackendStateImage = keyedStateHandles.iterator().next();

						}
						//TODO store secondary (local) states
					}
				}

				OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(
					subtaskStateReport.getManagedOperatorState(),
					subtaskStateReport.getRawOperatorState(),
					primaryKeyedBackendStateImage,
					subtaskStateReport.getRawKeyedState());

				subtaskStateByOperatorId.put(operatorID, operatorSubtaskState);
			}
		}

		TaskStateSnapshot taskStateSnapshot = subtaskStateByOperatorId.isEmpty() ?
			null : new TaskStateSnapshot(subtaskStateByOperatorId);

		checkpointResponder.acknowledgeCheckpoint(
			jobId,
			executionId,
			checkpointMetaData.getCheckpointId(),
			checkpointMetrics,
			taskStateSnapshot);
	}

	/**
	 * Tracking when local state can be disposed.
	 */
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		//TODO activate and prune secondary state images
	}

	/**
	 * Called when the slot is freed.
	 */
	public void release() {
		//TODO prune state images
	}
}
