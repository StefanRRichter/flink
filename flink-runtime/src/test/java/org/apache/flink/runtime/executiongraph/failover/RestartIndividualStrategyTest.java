/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

/**
 * Tests for RestartIndividualStrategyTest.
 */
public class RestartIndividualStrategyTest extends TestLogger {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	@Test(timeout = 30000)
	public void testRetryUnderNoResourceAvailableException() throws Exception {

		final Configuration jobManagerConfig = new Configuration();

		final int maxAttempts = 3;
		final long delayMillis = 100;

		jobManagerConfig.setInteger(
			RestartIndividualStrategy.RESTART_INDIVIDUAL_NO_RESOURCE_MAX_ATTEMPTS, maxAttempts);

		jobManagerConfig.setLong(
			RestartIndividualStrategy.RESTART_INDIVIDUAL_NO_RESOURCE_RESCHEDULE_DELAY_MS, delayMillis);

		jobManagerConfig.setString(
			JobManagerOptions.EXECUTION_FAILOVER_STRATEGY,
			FailoverStrategyLoader.INDIVIDUAL_RESTART_STRATEGY_NAME);

		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(NoOpInvokable.class);
		source.setParallelism(1);
		source.setSlotSharingGroup(new SlotSharingGroup());

		final JobGraph jobGraph = new JobGraph("test job", source);

		final Iterator<Boolean> availabilitySequence = Arrays.asList(true, false, false, true).iterator();

		final SimpleSlotProvider noSecondResourceSlotProvider = new SimpleSlotProvider(
			jobGraph.getJobID(),
			jobGraph.getMaximumParallelism()) {

			@Override
			public CompletableFuture<LogicalSlot> allocateSlot(
				SlotRequestId slotRequestId,
				ScheduledUnit task,
				boolean allowQueued,
				SlotProfile slotProfile,
				Time allocationTimeout) {

				if (availabilitySequence.hasNext() && availabilitySequence.next()) {
					return super.allocateSlot(slotRequestId, task, allowQueued, slotProfile, allocationTimeout);
				} else {
					return FutureUtils.completedExceptionally(new NoResourceAvailableException());
				}
			}
		};

		final DirectScheduledExecutorService futureExecutor = new DirectScheduledExecutorService();

		final ExecutionGraph executionGraph = createExecutionGraph(
			jobGraph,
			jobManagerConfig,
			noSecondResourceSlotProvider,
			futureExecutor);

		Assert.assertTrue(executionGraph.getFailoverStrategy() instanceof  RestartIndividualStrategy);

		RestartIndividualStrategy restartIndividualStrategy =
			(RestartIndividualStrategy) executionGraph.getFailoverStrategy();

		Assert.assertEquals(maxAttempts, restartIndividualStrategy.getMaxNoResourceReattempts());
		Assert.assertEquals(delayMillis, restartIndividualStrategy.getNoResourceRescheduleDelayMillis());

		ExecutionJobVertex jobVertex = executionGraph.getVerticesTopologically().iterator().next();
		ExecutionVertex executionVertex = jobVertex.getTaskVertices()[0];

		executionGraph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, executionGraph.getState());

		executionVertex.fail(new Exception());

		while (restartIndividualStrategy.getNumTaskFailures().getCount() < 3) {
			Thread.sleep(10);
		}

		assertEquals(JobStatus.RUNNING, executionGraph.getState());
		Assert.assertEquals(maxAttempts, restartIndividualStrategy.getRemainingNoResourceReattempts());

		executionVertex.fail(new Exception());

		while (executionGraph.getState() != JobStatus.FAILED) {
			Thread.sleep(10);
		}

		Assert.assertEquals(5, restartIndividualStrategy.getNumTaskFailures().getCount());
		Assert.assertEquals(0, restartIndividualStrategy.getRemainingNoResourceReattempts());
	}

	private ExecutionGraph createExecutionGraph(
		JobGraph jobGraph,
		Configuration jobManagerConfig,
		SlotProvider slotProvider,
		ScheduledExecutorService futureExecutor) throws JobException, JobExecutionException {

		final Time timeout = Time.seconds(10L);

		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			jobManagerConfig,
			futureExecutor,
			TestingUtils.defaultExecutor(),
			slotProvider,
			PipelinedFailoverRegionBuildingTest.class.getClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			timeout,
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			1000,
			VoidBlobWriter.getInstance(),
			timeout,
			log);
	}
}
