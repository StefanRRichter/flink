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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.ArrayList;
import java.util.List;

/**
 * Mock for interface {@link CheckpointResponder} for unit testing.
 */
public class CheckpointResponderMock implements CheckpointResponder {

	private final List<AcknowledgeReport> acknowledgeReports;
	private final List<DeclineReport> declineReports;

	private OneShotLatch acknowledgeLatch;
	private OneShotLatch declinedLatch;

	public CheckpointResponderMock() {
		this.acknowledgeReports = new ArrayList<>();
		this.declineReports = new ArrayList<>();
	}

	@Override
	public void acknowledgeCheckpoint(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		long checkpointId,
		CheckpointMetrics checkpointMetrics,
		TaskStateSnapshot subtaskState) {

		AcknowledgeReport acknowledgeReport = new AcknowledgeReport(
			jobID,
			executionAttemptID,
			checkpointId,
			checkpointMetrics,
			subtaskState);

		acknowledgeReports.add(acknowledgeReport);

		if (acknowledgeLatch != null) {
			acknowledgeLatch.trigger();
		}
	}

	@Override
	public void declineCheckpoint(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		long checkpointId,
		Throwable cause) {

		DeclineReport declineReport = new DeclineReport(
			jobID,
			executionAttemptID,
			checkpointId,
			cause);

		declineReports.add(declineReport);

		if (declinedLatch != null) {
			declinedLatch.trigger();
		}
	}

	public static class AcknowledgeReport {

		public final JobID jobID;
		public final ExecutionAttemptID executionAttemptID;
		public final long checkpointId;
		public final CheckpointMetrics checkpointMetrics;
		public final TaskStateSnapshot subtaskState;

		public AcknowledgeReport(
			JobID jobID,
			ExecutionAttemptID executionAttemptID,
			long checkpointId,
			CheckpointMetrics checkpointMetrics,
			TaskStateSnapshot subtaskState) {

			this.jobID = jobID;
			this.executionAttemptID = executionAttemptID;
			this.checkpointId = checkpointId;
			this.checkpointMetrics = checkpointMetrics;
			this.subtaskState = subtaskState;
		}
	}

	public static class DeclineReport {

		public final JobID jobID;
		public final ExecutionAttemptID executionAttemptID;
		public final long checkpointId;
		public final Throwable cause;

		public DeclineReport(
			JobID jobID,
			ExecutionAttemptID executionAttemptID,
			long checkpointId,
			Throwable cause) {

			this.jobID = jobID;
			this.executionAttemptID = executionAttemptID;
			this.checkpointId = checkpointId;
			this.cause = cause;
		}
	}

	public List<AcknowledgeReport> getAcknowledgeReports() {
		return acknowledgeReports;
	}

	public List<DeclineReport> getDeclineReports() {
		return declineReports;
	}

	public OneShotLatch getAcknowledgeLatch() {
		return acknowledgeLatch;
	}

	public void setAcknowledgeLatch(OneShotLatch acknowledgeLatch) {
		this.acknowledgeLatch = acknowledgeLatch;
	}

	public OneShotLatch getDeclinedLatch() {
		return declinedLatch;
	}

	public void setDeclinedLatch(OneShotLatch declinedLatch) {
		this.declinedLatch = declinedLatch;
	}

	public void clear() {
		acknowledgeReports.clear();
		declineReports.clear();
	}
}
