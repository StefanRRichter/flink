/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FutureUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed
 * and executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form
 * the Task's operator chain. Operators that are chained together execute synchronously in the
 * same thread and hence on the same stream partition. A common case for these chains
 * are successive map/flatmap/filter tasks.
 *
 * <p>The task chain contains one "head" operator and multiple chained operators.
 * The StreamTask is specialized for the type of the head operator: one-input and two-input tasks,
 * as well as for sources, iteration heads and iteration tails.
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * <p>The life cycle of the task is set up as follows:
 * <pre>{@code
 *  -- setInitialState -> provides state of all operators in the chain
 *
 *  -- invoke()
 *        |
 *        +----> Create basic utils (config, etc) and load the chain of operators
 *        +----> operators.setup()
 *        +----> task specific init()
 *        +----> initialize-operator-states()
 *        +----> open-operators()
 *        +----> run()
 *        +----> close-operators()
 *        +----> dispose-operators()
 *        +----> common cleanup
 *        +----> task specific cleanup()
 * }</pre>
 *
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a
 * {@code StreamOperator} must be synchronized on this lock object to ensure that no methods
 * are called concurrently.
 *
 * @param <OUT>
 * @param <OP>
 */
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
		extends AbstractInvokable
		implements StatefulTask, AsyncExceptionHandler {

	/** The thread group that holds all trigger timer threads. */
	public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

	/** The logger used by the StreamTask and its subclasses. */
	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	// ------------------------------------------------------------------------

	/**
	 * All interaction with the {@code StreamOperator} must be synchronized on this lock object to
	 * ensure that we don't have concurrent method calls that void consistent checkpoints.
	 */
	private final Object lock = new Object();

	/** the head operator that consumes the input streams of this task. */
	protected OP headOperator;

	/** The chain of operators executed by this task. */
	protected OperatorChain<OUT, OP> operatorChain;

	/** The configuration of this streaming task. */
	private StreamConfig configuration;

	/** Our state backend. We use this to create checkpoint streams and a keyed state backend. */
	private StateBackend stateBackend;

	/** Keyed state backend for the head operator, if it is keyed. There can only ever be one. */
	private AbstractKeyedStateBackend<?> keyedStateBackend;

	/**
	 * The internal {@link ProcessingTimeService} used to define the current
	 * processing time (default = {@code System.currentTimeMillis()}) and
	 * register timers for tasks to be executed in the future.
	 */
	private ProcessingTimeService timerService;

	/** The map of user-defined accumulators of this task. */
	private Map<String, Accumulator<?, ?>> accumulatorMap;

	private TaskStateHandles restoreStateHandles;

	/** The currently active background materialization threads. */
	private final CloseableRegistry cancelables = new CloseableRegistry();

	/**
	 * Flag to mark the task "in operation", in which case check needs to be initialized to true,
	 * so that early cancel() before invoke() behaves correctly.
	 */
	private volatile boolean isRunning;

	/** Flag to mark this task as canceled. */
	private volatile boolean canceled;

	/** Thread pool for async snapshot workers. */
	private ExecutorService asyncOperationsThreadPool;

	// ------------------------------------------------------------------------
	//  Life cycle methods for specific implementations
	// ------------------------------------------------------------------------

	protected abstract void init() throws Exception;

	protected abstract void run() throws Exception;

	protected abstract void cleanup() throws Exception;

	protected abstract void cancelTask() throws Exception;

	// ------------------------------------------------------------------------
	//  Core work methods of the Stream Task
	// ------------------------------------------------------------------------

	/**
	 * Allows the user to specify his own {@link ProcessingTimeService TimerServiceProvider}.
	 * By default a {@link SystemProcessingTimeService DefaultTimerService} is going to be provided.
	 * Changing it can be useful for testing processing time functionality, such as
	 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner WindowAssigners}
	 * and {@link org.apache.flink.streaming.api.windowing.triggers.Trigger Triggers}.
	 * */
	public void setProcessingTimeService(ProcessingTimeService timeProvider) {
		if (timeProvider == null) {
			throw new RuntimeException("The timeProvider cannot be set to null.");
		}
		timerService = timeProvider;
	}

	@Override
	public final void invoke() throws Exception {

		boolean disposed = false;
		try {
			// -------- Initialize ---------
			LOG.debug("Initializing {}.", getName());

			asyncOperationsThreadPool = Executors.newCachedThreadPool();

			configuration = new StreamConfig(getTaskConfiguration());

			stateBackend = createStateBackend();

			accumulatorMap = getEnvironment().getAccumulatorRegistry().getUserMap();

			// if the clock is not already set, then assign a default TimeServiceProvider
			if (timerService == null) {
				ThreadFactory timerThreadFactory =
					new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, "Time Trigger for " + getName());

				timerService = new SystemProcessingTimeService(this, getCheckpointLock(), timerThreadFactory);
			}

			operatorChain = new OperatorChain<>(this);
			headOperator = operatorChain.getHeadOperator();

			// task specific initialization
			init();

			// save the work of reloading state, etc, if the task is already canceled
			if (canceled) {
				throw new CancelTaskException();
			}

			// -------- Invoke --------
			LOG.debug("Invoking {}", getName());

			// we need to make sure that any triggers scheduled in open() cannot be
			// executed before all operators are opened
			synchronized (lock) {

				// both the following operations are protected by the lock
				// so that we avoid race conditions in the case that initializeState()
				// registers a timer, that fires before the open() is called.

				initializeState();
				openAllOperators();
			}

			// final check to exit early before starting to run
			if (canceled) {
				throw new CancelTaskException();
			}

			// let the task do its work
			isRunning = true;
			run();

			// if this left the run() method cleanly despite the fact that this was canceled,
			// make sure the "clean shutdown" is not attempted
			if (canceled) {
				throw new CancelTaskException();
			}

			// make sure all timers finish and no new timers can come
			timerService.quiesceAndAwaitPending();

			LOG.debug("Finished task {}", getName());

			// make sure no further checkpoint and notification actions happen.
			// we make sure that no other thread is currently in the locked scope before
			// we close the operators by trying to acquire the checkpoint scope lock
			// we also need to make sure that no triggers fire concurrently with the close logic
			// at the same time, this makes sure that during any "regular" exit where still
			synchronized (lock) {
				isRunning = false;

				// this is part of the main logic, so if this fails, the task is considered failed
				closeAllOperators();
			}

			LOG.debug("Closed operators for task {}", getName());

			// make sure all buffered data is flushed
			operatorChain.flushOutputs();

			// make an attempt to dispose the operators such that failures in the dispose call
			// still let the computation fail
			tryDisposeAllOperators();
			disposed = true;
		}
		finally {
			// clean up everything we initialized
			isRunning = false;

			// stop all timers and threads
			if (timerService != null) {
				try {
					timerService.shutdownService();
				}
				catch (Throwable t) {
					// catch and log the exception to not replace the original exception
					LOG.error("Could not shut down timer service", t);
				}
			}

			// stop all asynchronous checkpoint threads
			try {
				cancelables.close();
				shutdownAsyncThreads();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Could not shut down async checkpoint threads", t);
			}

			// we must! perform this cleanup
			try {
				cleanup();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Error during cleanup of stream task", t);
			}

			// if the operators were not disposed before, do a hard dispose
			if (!disposed) {
				disposeAllOperators();
			}

			// release the output resources. this method should never fail.
			if (operatorChain != null) {
				operatorChain.releaseOutputs();
			}
		}
	}

	@Override
	public final void cancel() throws Exception {
		isRunning = false;
		canceled = true;

		// the "cancel task" call must come first, but the cancelables must be
		// closed no matter what
		try {
			cancelTask();
		}
		finally {
			cancelables.close();
		}
	}

	public final boolean isRunning() {
		return isRunning;
	}

	public final boolean isCanceled() {
		return canceled;
	}

	/**
	 * Execute {@link StreamOperator#open()} of each operator in the chain of this
	 * {@link StreamTask}. Opening happens from <b>tail to head</b> operator in the chain, contrary
	 * to {@link StreamOperator#close()} which happens <b>head to tail</b>
	 * (see {@link #closeAllOperators()}.
	 */
	private void openAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.open();
			}
		}
	}

	/**
	 * Execute {@link StreamOperator#close()} of each operator in the chain of this
	 * {@link StreamTask}. Closing happens from <b>head to tail</b> operator in the chain,
	 * contrary to {@link StreamOperator#open()} which happens <b>tail to head</b>
	 * (see {@link #openAllOperators()}.
	 */
	private void closeAllOperators() throws Exception {
		// We need to close them first to last, since upstream operators in the chain might emit
		// elements in their close methods.
		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
		for (int i = allOperators.length - 1; i >= 0; i--) {
			StreamOperator<?> operator = allOperators[i];
			if (operator != null) {
				operator.close();
			}
		}
	}

	/**
	 * Execute {@link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 */
	private void tryDisposeAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.dispose();
			}
		}
	}

	private void shutdownAsyncThreads() throws Exception {
		if (!asyncOperationsThreadPool.isShutdown()) {
			asyncOperationsThreadPool.shutdownNow();
		}
	}

	/**
	 * Execute @link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 *
	 * <p>The difference with the {@link #tryDisposeAllOperators()} is that in case of an
	 * exception, this method catches it and logs the message.
	 */
	private void disposeAllOperators() {
		if (operatorChain != null) {
			for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
				try {
					if (operator != null) {
						operator.dispose();
					}
				}
				catch (Throwable t) {
					LOG.error("Error during disposal of stream operator.", t);
				}
			}
		}
	}

	/**
	 * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
	 * shutdown method was never called.
	 *
	 * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
	 * shutdown is attempted, and cause threads to linger for longer than needed.
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (timerService != null) {
			if (!timerService.isTerminated()) {
				LOG.info("Timer service is shutting down.");
				timerService.shutdownService();
			}
		}

		cancelables.close();
	}

	boolean isSerializingTimestamps() {
		TimeCharacteristic tc = configuration.getTimeCharacteristic();
		return tc == TimeCharacteristic.EventTime | tc == TimeCharacteristic.IngestionTime;
	}

	// ------------------------------------------------------------------------
	//  Access to properties and utilities
	// ------------------------------------------------------------------------

	/**
	 * Gets the name of the task, in the form "taskname (2/5)".
	 * @return The name of the task.
	 */
	public String getName() {
		return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
	}

	/**
	 * Gets the lock object on which all operations that involve data and state mutation have to lock.
	 * @return The checkpoint lock object.
	 */
	public Object getCheckpointLock() {
		return lock;
	}

	public StreamConfig getConfiguration() {
		return configuration;
	}

	public Map<String, Accumulator<?, ?>> getAccumulatorMap() {
		return accumulatorMap;
	}

	public StreamStatusMaintainer getStreamStatusMaintainer() {
		return operatorChain;
	}

	Output<StreamRecord<OUT>> getHeadOutput() {
		return operatorChain.getChainEntryPoint();
	}

	RecordWriterOutput<?>[] getStreamOutputs() {
		return operatorChain.getStreamOutputs();
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and Restore
	// ------------------------------------------------------------------------

	@Override
	public void setInitialState(TaskStateHandles taskStateHandles) {
		this.restoreStateHandles = taskStateHandles;
	}

	@Override
	public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {
		try {
			// No alignment if we inject a checkpoint
			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
					.setBytesBufferedInAlignment(0L)
					.setAlignmentDurationNanos(0L);

			return performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
		}
		catch (Exception e) {
			// propagate exceptions only if the task is still in "running" state
			if (isRunning) {
				throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() +
					" for operator " + getName() + '.', e);
			} else {
				LOG.debug("Could not perform checkpoint {} for operator {} while the " +
					"invokable was not in state running.", checkpointMetaData.getCheckpointId(), getName(), e);
				return false;
			}
		}
	}

	@Override
	public void triggerCheckpointOnBarrier(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		try {
			performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
		}
		catch (CancelTaskException e) {
			LOG.info("Operator {} was cancelled while performing checkpoint {}.",
					getName(), checkpointMetaData.getCheckpointId());
			throw e;
		}
		catch (Exception e) {
			throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() + " for operator " +
				getName() + '.', e);
		}
	}

	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws Exception {
		LOG.debug("Aborting checkpoint via cancel-barrier {} for task {}", checkpointId, getName());

		// notify the coordinator that we decline this checkpoint
		getEnvironment().declineCheckpoint(checkpointId, cause);

		// notify all downstream operators that they should not wait for a barrier from us
		synchronized (lock) {
			operatorChain.broadcastCheckpointCancelMarker(checkpointId);
		}
	}

	private boolean performCheckpoint(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		LOG.debug("Starting checkpoint ({}) {} on task {}",
			checkpointMetaData.getCheckpointId(), checkpointOptions.getCheckpointType(), getName());

		synchronized (lock) {
			if (isRunning) {
				// we can do a checkpoint

				// Since both state checkpointing and downstream barrier emission occurs in this
				// lock scope, they are an atomic operation regardless of the order in which they occur.
				// Given this, we immediately emit the checkpoint barriers, so the downstream operators
				// can start their checkpoint work as soon as possible
				operatorChain.broadcastCheckpointBarrier(
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						checkpointOptions);

				checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);
				return true;
			}
			else {
				// we cannot perform our checkpoint - let the downstream operators know that they
				// should not wait for any input from this operator

				// we cannot broadcast the cancellation markers on the 'operator chain', because it may not
				// yet be created
				final CancelCheckpointMarker message = new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
				Exception exception = null;

				for (ResultPartitionWriter output : getEnvironment().getAllWriters()) {
					try {
						output.writeBufferToAllChannels(EventSerializer.toBuffer(message));
					} catch (Exception e) {
						exception = ExceptionUtils.firstOrSuppressed(
							new Exception("Could not send cancel checkpoint marker to downstream tasks.", e),
							exception);
					}
				}

				if (exception != null) {
					throw exception;
				}

				return false;
			}
		}
	}

	public ExecutorService getAsyncOperationsThreadPool() {
		return asyncOperationsThreadPool;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (lock) {
			if (isRunning) {
				LOG.debug("Notification of complete checkpoint for task {}", getName());

				for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
					if (operator != null) {
						operator.notifyOfCompletedCheckpoint(checkpointId);
					}
				}
			}
			else {
				LOG.debug("Ignoring notification of complete checkpoint for not-running task {}", getName());
			}
		}
	}

	private void checkpointState(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		CheckpointingOperation checkpointingOperation = new CheckpointingOperation(
			this,
			checkpointMetaData,
			checkpointOptions,
			checkpointMetrics);

		cancelables.registerClosable(checkpointingOperation);

		try {
			checkpointingOperation.executeCheckpointing();
		} finally {
			cancelables.unregisterClosable(checkpointingOperation);
		}
	}

	private void initializeState() throws Exception {

		boolean restored = null != restoreStateHandles;

		if (restored) {
			checkRestorePreconditions(operatorChain.getChainLength());
			initializeOperators(true);
			restoreStateHandles = null; // free for GC
		} else {
			initializeOperators(false);
		}
	}

	private void initializeOperators(boolean restored) throws Exception {
		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
		for (int chainIdx = 0; chainIdx < allOperators.length; ++chainIdx) {
			StreamOperator<?> operator = allOperators[chainIdx];
			if (null != operator) {
				if (restored && restoreStateHandles != null) {
					operator.initializeState(new OperatorStateHandles(restoreStateHandles, chainIdx));
				} else {
					operator.initializeState(null);
				}
			}
		}
	}

	private void checkRestorePreconditions(int operatorChainLength) {

		ChainedStateHandle<StreamStateHandle> nonPartitionableOperatorStates =
				restoreStateHandles.getLegacyOperatorState();
		List<Collection<OperatorStateHandle>> operatorStates =
				restoreStateHandles.getManagedOperatorState();

		if (nonPartitionableOperatorStates != null) {
			Preconditions.checkState(nonPartitionableOperatorStates.getLength() == operatorChainLength,
					"Invalid Invalid number of operator states. Found :" + nonPartitionableOperatorStates.getLength()
							+ ". Expected: " + operatorChainLength);
		}

		if (!CollectionUtil.isNullOrEmpty(operatorStates)) {
			Preconditions.checkArgument(operatorStates.size() == operatorChainLength,
					"Invalid number of operator states. Found :" + operatorStates.size() +
							". Expected: " + operatorChainLength);
		}
	}

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------

	private StateBackend createStateBackend() throws Exception {
		final StateBackend fromJob = configuration.getStateBackend(getUserCodeClassLoader());

		if (fromJob != null) {
			// backend has been configured on the environment
			LOG.info("Using user-defined state backend: {}.", fromJob);
			return fromJob;
		}
		else {
			return AbstractStateBackend.loadStateBackendFromConfigOrCreateDefault(
					getEnvironment().getTaskManagerInfo().getConfiguration(),
					getUserCodeClassLoader(),
					LOG);
		}
	}

	public OperatorStateBackend createOperatorStateBackend(
			StreamOperator<?> op, Collection<OperatorStateHandle> restoreStateHandles) throws Exception {

		Environment env = getEnvironment();
		String opId = createOperatorIdentifier(op, getConfiguration().getVertexID());

		OperatorStateBackend operatorStateBackend = stateBackend.createOperatorStateBackend(env, opId);

		// let operator state backend participate in the operator lifecycle, i.e. make it responsive to cancelation
		cancelables.registerClosable(operatorStateBackend);

		// restore if we have some old state
		if (null != restoreStateHandles) {
			operatorStateBackend.restore(restoreStateHandles);
		}

		return operatorStateBackend;
	}

	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange) throws Exception {

		if (keyedStateBackend != null) {
			throw new RuntimeException("The keyed state backend can only be created once.");
		}

		String operatorIdentifier = createOperatorIdentifier(
				headOperator,
				configuration.getVertexID());

		keyedStateBackend = stateBackend.createKeyedStateBackend(
				getEnvironment(),
				getEnvironment().getJobID(),
				operatorIdentifier,
				keySerializer,
				numberOfKeyGroups,
				keyGroupRange,
				getEnvironment().getTaskKvStateRegistry());

		// let keyed state backend participate in the operator lifecycle, i.e. make it responsive to cancelation
		cancelables.registerClosable(keyedStateBackend);

		// restore if we have some old state
		Collection<KeyedStateHandle> restoreKeyedStateHandles =
			restoreStateHandles == null ? null : restoreStateHandles.getManagedKeyedState();

		keyedStateBackend.restore(restoreKeyedStateHandles);

		@SuppressWarnings("unchecked")
		AbstractKeyedStateBackend<K> typedBackend = (AbstractKeyedStateBackend<K>) keyedStateBackend;
		return typedBackend;
	}

	/**
	 * This is only visible because
	 * {@link org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink} uses the
	 * checkpoint stream factory to write write-ahead logs. <b>This should not be used for
	 * anything else.</b>
	 */
	public CheckpointStreamFactory createCheckpointStreamFactory(StreamOperator<?> operator) throws IOException {
		return stateBackend.createStreamFactory(
				getEnvironment().getJobID(),
				createOperatorIdentifier(operator, configuration.getVertexID()));
	}

	public CheckpointStreamFactory createSavepointStreamFactory(StreamOperator<?> operator, String targetLocation) throws IOException {
		return stateBackend.createSavepointStreamFactory(
			getEnvironment().getJobID(),
			createOperatorIdentifier(operator, configuration.getVertexID()),
			targetLocation);
	}

	private String createOperatorIdentifier(StreamOperator<?> operator, int vertexId) {
		return operator.getClass().getSimpleName() +
				"_" + vertexId +
				"_" + getEnvironment().getTaskInfo().getIndexOfThisSubtask();
	}

	/**
	 * Returns the {@link ProcessingTimeService} responsible for telling the current
	 * processing time and registering timers.
	 */
	public ProcessingTimeService getProcessingTimeService() {
		if (timerService == null) {
			throw new IllegalStateException("The timer service has not been initialized.");
		}
		return timerService;
	}

	/**
	 * Handles an exception thrown by another thread (e.g. a TriggerTask),
	 * other than the one executing the main task by failing the task entirely.
	 *
	 * <p>In more detail, it marks task execution failed for an external reason
	 * (a reason other than the task code itself throwing an exception). If the task
	 * is already in a terminal state (such as FINISHED, CANCELED, FAILED), or if the
	 * task is already canceling this does nothing. Otherwise it sets the state to
	 * FAILED, and, if the invokable code is running, starts an asynchronous thread
	 * that aborts that code.
	 *
	 * <p>This method never blocks.</p>
	 */
	@Override
	public void handleAsyncException(String message, Throwable exception) {
		getEnvironment().failExternally(new Throwable(message, exception));
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return getName();
	}

	public CloseableRegistry getCancelables() {
		return cancelables;
	}

	// ------------------------------------------------------------------------

	private static final class CheckpointingOperation implements Closeable {

		private enum CheckpointOperationStatus {
			RUNNING,
			DISCARDED,
			COMPLETED
		}

		private final StreamTask<?, ?> owner;

		private final CheckpointMetaData checkpointMetaData;
		private final CheckpointOptions checkpointOptions;
		private final CheckpointMetrics checkpointMetrics;

		private final StreamOperator<?>[] allOperators;

		private SubtaskState subtaskState;

		private final AtomicReference<CheckpointOperationStatus> checkpointStatus;

		private long startSyncPartNano;
		private long startAsyncPartNano;

		// ------------------------

		private final List<StreamStateHandle> nonPartitionedStates;
		private final List<OperatorSnapshotFutures> snapshotInProgressList;

		public CheckpointingOperation(
				StreamTask<?, ?> owner,
				CheckpointMetaData checkpointMetaData,
				CheckpointOptions checkpointOptions,
				CheckpointMetrics checkpointMetrics) {

			this.owner = Preconditions.checkNotNull(owner);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointOptions = Preconditions.checkNotNull(checkpointOptions);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.allOperators = owner.operatorChain.getAllOperators();
			this.nonPartitionedStates = new ArrayList<>(allOperators.length);
			this.snapshotInProgressList = new ArrayList<>(allOperators.length);
			this.checkpointStatus = new AtomicReference<>(CheckpointOperationStatus.RUNNING);
			this.subtaskState = null;
		}

		public void executeCheckpointing() {

			try {

				try {
					runBlockingPart();
				} catch (CancellationException cex) {
					logCancellation(cex);
					return;
				} catch (Throwable t) {
					owner.handleAsyncException("Exception in synchronous part of snapshot", t);
					return;
				}

				try {
					runNonBlockingPart();
				} catch (CancellationException cex) {
					logCancellation(cex);
					return;
				} catch (Throwable t) {
					owner.handleAsyncException("Exception in asynchronous part of snapshot", t);
					return;
				}

				if (checkpointStatus.compareAndSet(
					CheckpointOperationStatus.RUNNING,
					CheckpointOperationStatus.COMPLETED)) {

					acknowledgeCompletedSnapshot();
				} else {
					logCancellation(
						new CancellationException("Snapshot was canceled before acknowledgment."));
				}

			} finally {
				if (checkpointStatus.get() != CheckpointOperationStatus.COMPLETED) {
					cancelAndCleanup();
				}
			}
		}

		@Override
		public void close() throws IOException {
			cancelAndCleanup();
		}

		private void runBlockingPart() throws Exception {

			startSyncPartNano = System.nanoTime();

			for (StreamOperator<?> op : allOperators) {

				// Fail fast.
				if (checkpointStatus.get() == CheckpointOperationStatus.DISCARDED) {
					throw new CancellationException("Received cancel in synchronous part of snapshot.");
				}

				snapshotStreamOperator(op);
			}

			startAsyncPartNano = System.nanoTime();
			checkpointMetrics.setSyncDurationMillis((startAsyncPartNano - startSyncPartNano) / 1_000_000);

			LOG.debug("{} - finished synchronous part of snapshot {}." +
					"Alignment duration: {} ms, snapshot duration {} ms",
				owner.getName(), checkpointMetaData.getCheckpointId(),
				checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
				checkpointMetrics.getSyncDurationMillis());
		}

		private void runNonBlockingPart() throws ExecutionException, InterruptedException {

			KeyedStateHandle keyedStateHandleBackend = null;
			KeyedStateHandle keyedStateHandleStream = null;

			if (!snapshotInProgressList.isEmpty()) {
				// TODO Currently only the head operator of a chain can have keyed state, so simply access it directly.
				int headIndex = snapshotInProgressList.size() - 1;
				OperatorSnapshotFutures snapshotInProgress = snapshotInProgressList.get(headIndex);
				if (snapshotInProgress != null) {
					// Keyed state handle future, currently only one (the head) operator can have this
					keyedStateHandleBackend =
						FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getKeyedStateManagedFuture());
					keyedStateHandleStream =
						FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getKeyedStateRawFuture());
				}
			}

			List<OperatorStateHandle> operatorStatesBackend = new ArrayList<>(snapshotInProgressList.size());
			List<OperatorStateHandle> operatorStatesStream = new ArrayList<>(snapshotInProgressList.size());

			for (OperatorSnapshotFutures snapshotInProgress : snapshotInProgressList) {
				if (snapshotInProgress != null) {
					operatorStatesBackend.add(
						FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getOperatorStateManagedFuture()));
					operatorStatesStream.add(
						FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getOperatorStateRawFuture()));
				} else {
					operatorStatesBackend.add(null);
					operatorStatesStream.add(null);
				}
			}

			ChainedStateHandle<StreamStateHandle> chainedNonPartitionedOperatorsState =
				new ChainedStateHandle<>(nonPartitionedStates);

			ChainedStateHandle<OperatorStateHandle> chainedOperatorStateBackend =
				new ChainedStateHandle<>(operatorStatesBackend);

			ChainedStateHandle<OperatorStateHandle> chainedOperatorStateStream =
				new ChainedStateHandle<>(operatorStatesStream);

			this.subtaskState = createSubtaskStateFromSnapshotStateHandles(
				chainedNonPartitionedOperatorsState,
				chainedOperatorStateBackend,
				chainedOperatorStateStream,
				keyedStateHandleBackend,
				keyedStateHandleStream);

			final long asyncEndNanos = System.nanoTime();
			final long asyncDurationMillis = (asyncEndNanos - startAsyncPartNano) / 1_000_000;

			checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);

			LOG.debug("{} - finished asynchronous part of snapshot {}. Asynchronous duration: {} ms",
				owner.getName(), checkpointMetaData.getCheckpointId(), asyncDurationMillis);

		}

		private void acknowledgeCompletedSnapshot() {
			owner.getEnvironment().acknowledgeCheckpoint(
				checkpointMetaData.getCheckpointId(),
				checkpointMetrics,
				subtaskState);
		}

		@SuppressWarnings("deprecation")
		private void snapshotStreamOperator(StreamOperator<?> op) throws Exception {
			if (op != null) {
				// first call the legacy checkpoint code paths
				nonPartitionedStates.add(op.snapshotLegacyOperatorState(
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						checkpointOptions));

				OperatorSnapshotFutures snapshotInProgress = op.snapshotState(
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						checkpointOptions);

				snapshotInProgressList.add(snapshotInProgress);
			} else {
				nonPartitionedStates.add(null);
				OperatorSnapshotFutures emptySnapshotInProgress = new OperatorSnapshotFutures();
				snapshotInProgressList.add(emptySnapshotInProgress);
			}
		}

		private void cancelAndCleanup() {

			if (!checkpointStatus.compareAndSet(
				CheckpointOperationStatus.RUNNING,
				CheckpointOperationStatus.DISCARDED)) {

				// already canceled or already completed.
				return;
			}

			// Cleanup to release resources
			for (OperatorSnapshotFutures operatorSnapshotResult : snapshotInProgressList) {
				if (operatorSnapshotResult != null) {
					try {
						operatorSnapshotResult.cancel();
					} catch (Exception e) {
						LOG.warn("Could not properly cancel an operator snapshot result.", e);
					}
				}
			}

			// Cleanup non partitioned state handles
			for (StreamStateHandle nonPartitionedState : nonPartitionedStates) {
				if (nonPartitionedState != null) {
					try {
						nonPartitionedState.discardState();
					} catch (Exception e) {
						LOG.warn("Could not properly discard a non partitioned " +
							"state. This might leave some orphaned files behind.", e);
					}
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("{} - FAILED to finish snapshot {}." +
						"Alignment duration: {} ms, snapshot duration {} ms",
					owner.getName(), checkpointMetaData.getCheckpointId(),
					checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
					checkpointMetrics.getSyncDurationMillis());
			}
		}

		private SubtaskState createSubtaskStateFromSnapshotStateHandles(
			ChainedStateHandle<StreamStateHandle> chainedNonPartitionedOperatorsState,
			ChainedStateHandle<OperatorStateHandle> chainedOperatorStateBackend,
			ChainedStateHandle<OperatorStateHandle> chainedOperatorStateStream,
			KeyedStateHandle keyedStateHandleBackend,
			KeyedStateHandle keyedStateHandleStream) {

			boolean hasAnyState = keyedStateHandleBackend != null
				|| keyedStateHandleStream != null
				|| !chainedOperatorStateBackend.isEmpty()
				|| !chainedOperatorStateStream.isEmpty()
				|| !chainedNonPartitionedOperatorsState.isEmpty();

			// we signal a stateless task by reporting null, so that there are no attempts to assign empty state to
			// stateless tasks on restore. This allows for simple job modifications that only concern stateless without
			// the need to assign them uids to match their (always empty) states.
			return hasAnyState ? new SubtaskState(
				chainedNonPartitionedOperatorsState,
				chainedOperatorStateBackend,
				chainedOperatorStateStream,
				keyedStateHandleBackend,
				keyedStateHandleStream)
				: null;
		}

		private void logCancellation(CancellationException cex) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(owner.getName() + " - snapshot " + checkpointMetaData.getCheckpointId() +
						" could not be completed because it was canceled.",
					cex);
			}
		}
	}
}
