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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.GlobalModVersionMismatch;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Simple failover strategy that restarts each task individually.
 * This strategy is only applicable if the entire job consists unconnected
 * tasks, meaning each task is its own component.
 */
public class RestartIndividualStrategy extends FailoverStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(RestartIndividualStrategy.class);

	/**
	 * ConfigOption for the maximum number of consecutive re-attempts when restarting from
	 * {@link NoResourceAvailableException}.
	 */
	public static final ConfigOption<Integer> RESTART_INDIVIDUAL_NO_RESOURCE_MAX_ATTEMPTS = ConfigOptions
		.key("failover_strategy.restart_individual.no_resource_max_attempts")
		.defaultValue(0)
		.withDescription("This non-negative integer parameter determines the maximum number of consecutive " +
			"unsuccessful restarts caused by NoResourceAvailableException. Default is 0.");

	/**
	 * ConfigOption for the rescheduling delay when restarting from {@link NoResourceAvailableException}.
	 */
	public static final ConfigOption<Long> RESTART_INDIVIDUAL_NO_RESOURCE_RESCHEDULE_DELAY_MS = ConfigOptions
		.key("failover_strategy.restart_individual.no_resource_reschedule_delay_ms")
		.defaultValue(0L)
		.withDescription("This non-negative long parameter determines the rescheduling delay for restarts caused by " +
			"NoResourceAvailableException, in milliseconds. Default is 0 milliseconds.");

	// ------------------------------------------------------------------------

	/** The execution graph to recover */
	@Nonnull
	private final ExecutionGraph executionGraph;

	/** The executor that executes restart callbacks */
	@Nonnull
	private final ScheduledExecutor callbackExecutor;

	@Nonnull
	private final SimpleCounter numTaskFailures;

	/** The delay before a restart is executed if the failure cause was a NoResourceAvailableException */
	@Nonnegative
	private final long noResourceRescheduleDelayMillis;

	/** The maximum number of re-attempts for failures from NoResourceAvailableException */
	@Nonnegative
	private final int maxNoResourceReattempts;

	/**
	 * The number of remaining re-attempts for failures from NoResourceAvailableException. Successful restarts reset
	 * this number to maxNoResourceReattempts
	 */
	@Nonnegative
	private int remainingNoResourceReattempts;

	/**
	 * Creates a new failover strategy that recovers from failures by restarting only the failed task
	 * of the execution graph.
	 *
	 * @param executionGraph The execution graph to handle.
	 * @param callbackExecutor The executor that executes restart callbacks
	 */
	public RestartIndividualStrategy(
		@Nonnull ExecutionGraph executionGraph,
		@Nonnull ScheduledExecutor callbackExecutor) {

		this(
			executionGraph,
			callbackExecutor,
			0,
			0);
	}

	/**
	 * Creates a new failover strategy that recovers from failures by restarting only the failed task
	 * of the execution graph.
	 *
	 * @param executionGraph The execution graph to handle.
	 * @param callbackExecutor The executor that executes restart callbacks.
	 * @param maxNoResourceReattempts maximum number of restart attempts when failed from {@link NoResourceAvailableException}.
	 * @param noResourceRescheduleDelayMillis the delay to reschedule a restart when failed from {@link NoResourceAvailableException}.
	 */
	@VisibleForTesting
	RestartIndividualStrategy(
		@Nonnull ExecutionGraph executionGraph,
		@Nonnull ScheduledExecutor callbackExecutor,
		@Nonnegative int maxNoResourceReattempts,
		@Nonnegative long noResourceRescheduleDelayMillis) {

		this.executionGraph = executionGraph;
		this.callbackExecutor = callbackExecutor;
		this.noResourceRescheduleDelayMillis = noResourceRescheduleDelayMillis;
		this.maxNoResourceReattempts = maxNoResourceReattempts;
		this.remainingNoResourceReattempts = maxNoResourceReattempts;
		this.numTaskFailures = new SimpleCounter();
	}

	// ------------------------------------------------------------------------

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		// to better handle the lack of resources (potentially by a scale-in), we
		// make failures due to missing resources global failures 
		if (cause instanceof NoResourceAvailableException) {

			if (remainingNoResourceReattempts > 0) {

				LOG.info("Not enough resources to schedule {} - retrying individual recovery. {} of {} attempts left.",
					remainingNoResourceReattempts, maxNoResourceReattempts, taskExecution);

				--remainingNoResourceReattempts;

				// we schedule restart with the configured delay, so that resources have some time to become available
				callbackExecutor.schedule(
					() -> tryIndividualRestartTask(taskExecution),
					noResourceRescheduleDelayMillis,
					TimeUnit.MILLISECONDS);
			} else {

				LOG.info("Not enough resources to schedule {} - triggering full recovery.", taskExecution);
				executionGraph.failGlobal(cause);
			}
		} else {

			tryIndividualRestartTask(taskExecution);
		}
	}

	@Override
	public void onSuccessfulRecovery() {
		// after a successful restart, we reset the retry-attempts
		remainingNoResourceReattempts = maxNoResourceReattempts;
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		// we validate here that the vertices are in fact not connected to
		// any other vertices
		for (ExecutionJobVertex ejv : newJobVerticesTopological) {
			List<IntermediateResult> inputs = ejv.getInputs();
			IntermediateResult[] outputs = ejv.getProducedDataSets();

			if ((inputs != null && inputs.size() > 0) || (outputs != null && outputs.length > 0)) {
				throw new FlinkRuntimeException("Incompatible failover strategy - strategy '" + 
						getStrategyName() + "' can only handle jobs with only disconnected tasks.");
			}
		}
	}

	@Override
	public String getStrategyName() {
		return "Individual Task Restart";
	}

	@Override
	public void registerMetrics(MetricGroup metricGroup) {
		metricGroup.counter("task_failures", numTaskFailures);
	}

	@Nonnull
	@VisibleForTesting
	SimpleCounter getNumTaskFailures() {
		return numTaskFailures;
	}

	@VisibleForTesting
	long getNoResourceRescheduleDelayMillis() {
		return noResourceRescheduleDelayMillis;
	}

	@VisibleForTesting
	int getMaxNoResourceReattempts() {
		return maxNoResourceReattempts;
	}

	@VisibleForTesting
	int getRemainingNoResourceReattempts() {
		return remainingNoResourceReattempts;
	}

	private void tryIndividualRestartTask(Execution taskExecution) {
		LOG.info("Recovering task failure for {} (#{}) via individual restart.",
			taskExecution.getVertex().getTaskNameWithSubtaskIndex(), taskExecution.getAttemptNumber());
		numTaskFailures.inc();

		// trigger the restart once the task has reached its terminal state
		// Note: currently all tasks passed here are already in their terminal state,
		//       so we could actually avoid the future. We use it anyways because it is cheap and
		//       it helps to support better testing
		final CompletableFuture<ExecutionState> terminationFuture = taskExecution.getTerminalStateFuture();

		final ExecutionVertex vertexToRecover = taskExecution.getVertex();
		final long globalModVersion = taskExecution.getGlobalModVersion();

		terminationFuture.thenAcceptAsync(
			(ExecutionState value) -> {
				try {
					long createTimestamp = System.currentTimeMillis();
					Execution newExecution = vertexToRecover.resetForNewExecution(createTimestamp, globalModVersion);
					newExecution.scheduleForExecution();
				}
				catch (GlobalModVersionMismatch e) {
					// this happens if a concurrent global recovery happens. simply do nothing.
				}
				catch (Exception e) {
					executionGraph.failGlobal(
						new Exception("Error during fine grained recovery - triggering full recovery", e));
				}
			},
			callbackExecutor);
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * Factory that instantiates the RestartAllStrategy.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Nonnull
		private final Configuration jobManagerConfig;

		public Factory(@Nonnull Configuration jobManagerConfig) {
			this.jobManagerConfig = jobManagerConfig;
		}

		@Override
		public RestartIndividualStrategy create(ExecutionGraph executionGraph) {

			ScheduledExecutor futureExecutor = executionGraph.getScheduledFutureExecutor();

			int noResourceMaxAttempts = jobManagerConfig.getInteger(
				RESTART_INDIVIDUAL_NO_RESOURCE_MAX_ATTEMPTS.key(),
				RESTART_INDIVIDUAL_NO_RESOURCE_MAX_ATTEMPTS.defaultValue());

			long noResourceRescheduleDelay = jobManagerConfig.getLong(
				RESTART_INDIVIDUAL_NO_RESOURCE_RESCHEDULE_DELAY_MS.key(),
				RESTART_INDIVIDUAL_NO_RESOURCE_RESCHEDULE_DELAY_MS.defaultValue());

			return new RestartIndividualStrategy(
				executionGraph,
				futureExecutor,
				noResourceMaxAttempts,
				noResourceRescheduleDelay);
		}
	}
}
