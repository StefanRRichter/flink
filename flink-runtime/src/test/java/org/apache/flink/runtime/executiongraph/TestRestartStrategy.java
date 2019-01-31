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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.restart.RestartCallback;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;

import javax.annotation.Nonnull;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 *
 */
public class TestRestartStrategy implements RestartStrategy {

	@Nonnull
	private final Queue<ExecutorAction> actionsQueue;

	@Nonnull
	private BooleanSupplier canRestartCheck;

	private boolean manuallyTriggeredExecution;

	public TestRestartStrategy() {
		this(true);
	}

	public TestRestartStrategy(boolean manuallyTriggeredExecution) {
		this(() -> true, manuallyTriggeredExecution);
	}

	public TestRestartStrategy(@Nonnull BooleanSupplier canRestartCheck, boolean manuallyTriggeredExecution) {
		this(new LinkedList<>(), canRestartCheck, manuallyTriggeredExecution);
	}

	public TestRestartStrategy(@Nonnull Queue<ExecutorAction> actionsQueue, @Nonnull BooleanSupplier canRestartCheck, boolean manuallyTriggeredExecution) {
		this.actionsQueue = actionsQueue;
		this.canRestartCheck = canRestartCheck;
		this.manuallyTriggeredExecution = manuallyTriggeredExecution;
	}

	@Override
	public boolean canRestart() {
		return canRestartCheck.getAsBoolean();
	}

	@Override
	public void restart(RestartCallback restarter, ScheduledExecutor executor) {

		ExecutorAction executorAction = new ExecutorAction(restarter::triggerFullRecovery, executor);

		if (manuallyTriggeredExecution) {
			synchronized (actionsQueue) {
				actionsQueue.add(executorAction);
			}
		} else {
			executorAction.trigger();
		}
	}

	public int getNumberOfQueuedActions() {
		synchronized (actionsQueue) {
			return actionsQueue.size();
		}
	}

	public CompletableFuture<Void> triggerNextAction() {
		synchronized (actionsQueue) {
			return actionsQueue.remove().trigger();
		}
	}

	public CompletableFuture<Void> triggerAll() {

		synchronized (actionsQueue) {

			if (actionsQueue.isEmpty()) {
				return CompletableFuture.completedFuture(null);
			}

			CompletableFuture<?>[] completableFutures = new CompletableFuture[actionsQueue.size()];
			for (int i = 0; i < completableFutures.length; ++i) {
				completableFutures[i] = triggerNextAction();
			}
			return FutureUtils.ConjunctFuture.allOf(completableFutures);
		}
	}

	public boolean isManuallyTriggeredExecution() {
		return manuallyTriggeredExecution;
	}

	public void setManuallyTriggeredExecution(boolean manuallyTriggeredExecution) {
		this.manuallyTriggeredExecution = manuallyTriggeredExecution;
	}

	public static TestRestartStrategy manuallyTriggered() {
		return new TestRestartStrategy(true);
	}

	public static TestRestartStrategy directExecuting() {
		return new TestRestartStrategy(false);
	}

	private static class ExecutorAction {

		final Runnable runnable;
		final Executor executor;

		ExecutorAction(Runnable runnable, Executor executor) {
			this.runnable = runnable;
			this.executor = executor;
		}

		public CompletableFuture<Void> trigger() {
			return CompletableFuture.runAsync(runnable, executor);
		}
	}
}
