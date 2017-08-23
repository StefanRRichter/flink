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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class TaskStateManager {

	private final Map<JobID, SlotStateManager> slotStateManagers;

	public TaskStateManager() {
		this.slotStateManagers = new HashMap<>();
	}

	public SlotStateManager getSlotStateManager(
		JobID jobId,
		ExecutionAttemptID executionAttemptId,
		CheckpointResponder checkpointResponder) {

		Preconditions.checkNotNull(jobId);
		Preconditions.checkNotNull(executionAttemptId);
		Preconditions.checkNotNull(checkpointResponder);

		SlotStateManager previousSlotStateManager = slotStateManagers.get(jobId);

		SlotStateManager newSlotStateManager = new SlotStateManager(jobId, executionAttemptId, checkpointResponder);

		if (previousSlotStateManager != null) {
			//TODO transfer state from previous to new.
		}

		return newSlotStateManager;
	}

	public SlotStateManager release(JobID jobID) {
		return slotStateManagers.remove(jobID);
	}
}
