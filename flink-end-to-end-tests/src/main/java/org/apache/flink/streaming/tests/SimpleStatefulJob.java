/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Simple test.
 */
public class SimpleStatefulJob {

	private static final String ALLOCATION_ID_EXTRACTION_PATTERN = ".*allocationID=(.*?),.*";
	private static final Logger LOG = LoggerFactory.getLogger(SimpleStatefulJob.class);

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool pt = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(pt.getInt("checkpointInterval", 1000));

		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, pt.getInt("restartDelay", 0)));
		if (pt.has("externalizedCheckpoints") && pt.getBoolean("externalizedCheckpoints", false)) {
			env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		}

		int parallelism = pt.getInt("parallelism", 1);
		env.setParallelism(parallelism);
		env.setMaxParallelism(pt.getInt("maxParallelism", pt.getInt("parallelism", 1)));

		String stateBackend = pt.get("stateBackend", "file");
		String checkpointDir = pt.getRequired("checkpointDir");

		if ("file".equals(stateBackend)) {
			boolean asyncCheckpoints = pt.getBoolean("asyncCheckpoints", false);
			env.setStateBackend(new FsStateBackend(checkpointDir, asyncCheckpoints));
		} else {
			boolean incrementalCheckpoints = pt.getBoolean("incrementalCheckpoints", false);
			env.setStateBackend(new RocksDBStateBackend(checkpointDir, incrementalCheckpoints));
		}

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(pt);

		long delay = pt.has("delay") ? pt.getLong("delay") : 0L;
		int maxAttempts = pt.has("maxAttempts") ? pt.getInt("maxAttempts") : 3;
		int valueSize = pt.has("valueSize") ? pt.getInt("valueSize") : 10;

		env.addSource(new RandomLongSource(maxAttempts, delay))
			.setParallelism(1)
			.keyBy((KeySelector<Long, Long>) aLong -> aLong)
			.flatMap(new StateCreatingFlatMap(valueSize))
			.setParallelism(parallelism)
			.map(new NopMap())
			.setParallelism(parallelism)
			.addSink(new PrintSinkFunction<>());

		env.execute("Sticky Allocation Test");
	}

	/**
	 * Source.
	 */
	private static final class RandomLongSource extends RichSourceFunction<Long> implements CheckpointedFunction {

		private static final long serialVersionUID = 1L;

		final long delay;
		final int maxAttempts;

		ListState<Long> state;
		long currentKey;

		volatile boolean running = true;

		RandomLongSource(int maxAttempts, long delay) {
			this.delay = delay;
			this.maxAttempts = maxAttempts;
		}

		private String getTaskAllocationId() {
			StreamingRuntimeContext streamingRuntimeContext = (StreamingRuntimeContext) getRuntimeContext();
			TaskStateManager taskStateManager = streamingRuntimeContext.getTaskEnvironment().getTaskStateManager();
			return String.valueOf(taskStateManager).replaceAll(ALLOCATION_ID_EXTRACTION_PATTERN, "$1");
		}

		@Override
		public void run(SourceContext<Long> sourceContext) throws Exception {

			int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
			int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();

			if (getRuntimeContext().getAttemptNumber() > maxAttempts) {
				sourceContext.collect(Long.MAX_VALUE - subtaskIdx);
				return;
			}

			while (true) {
				sourceContext.collect(currentKey);
				currentKey += numberOfParallelSubtasks;

				if (delay > 0) {
					Thread.sleep(delay);
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.clear();
			state.add(currentKey);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {

			System.out.println(getRuntimeContext().getAttemptNumber() + "> Starting " + getRuntimeContext().getTaskNameWithSubtasks() + " " + getTaskAllocationId() + " " + getJvmPid());

			ListStateDescriptor<Long> currentKeyDescriptor = new ListStateDescriptor<>("currentKey", Long.class);
			state = context.getOperatorStateStore().getListState(currentKeyDescriptor);

			currentKey = getRuntimeContext().getIndexOfThisSubtask();
			Iterable<Long> iterable = state.get();
			if (iterable != null) {
				Iterator<Long> iterator = iterable.iterator();
				if (iterator.hasNext()) {
					currentKey = iterator.next();
					Preconditions.checkState(!iterator.hasNext());
				}
			}
		}
	}

	/**
	 * Map.
	 */
	private static final class StateCreatingFlatMap
		extends RichFlatMapFunction<Long, String> implements CheckpointedFunction, CheckpointListener {

		private static final long serialVersionUID = 1L;

		final int valueSize;

		transient ValueState<String> valueState;
		transient ListState<MapperTestInfo> mapperState;

		transient volatile boolean signalTestFailure;
		transient volatile boolean failTask;
//		transient volatile boolean blockNextSnapshot;

		StateCreatingFlatMap(int valueSize) {
			this.valueSize = valueSize;
			this.signalTestFailure = false;
			this.failTask = false;
//			this.blockNextSnapshot = false;
		}

		@Override
		public void flatMap(Long key, Collector<String> collector) throws IOException {

			if (signalTestFailure) {
				collector.collect("Test failed: task was rescheduled to unexpected allocation.");
				signalTestFailure = false;
			}

			if (failTask) {
				failTask = false;
				throw new RuntimeException("Artificial failure.");
			}

//			if (null != valueState.value()) {
//				throw new IllegalStateException("This should never happen, keys are generated monotonously.");
//			}

			valueState.update(RandomStringUtils.random(valueSize, true, true));
		}

		@Override
		public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

//			if (blockNextSnapshot) {
//				while (true) {
//					Thread.sleep(100);
//				}
//			} else {
//				blockNextSnapshot = true;
//			}

			MapperTestInfo mapperTestInfo = new MapperTestInfo(
				shouldTaskFailForCheckpoint(functionSnapshotContext.getCheckpointId()),
				getJvmPid(),
				getRuntimeContext().getTaskNameWithSubtasks(),
				getTaskAllocationId());

			System.out.println("Snapshot chk " + functionSnapshotContext.getCheckpointId() + " as " + mapperTestInfo);

			mapperState.clear();
			mapperState.add(mapperTestInfo);
		}

		@Override
		public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
			StreamingRuntimeContext streamingRuntimeContext = (StreamingRuntimeContext) getRuntimeContext();
			Configuration configuration = streamingRuntimeContext.getTaskEnvironment().getTaskManagerInfo().getConfiguration();
			System.out.println(getRuntimeContext().getAttemptNumber() + "> Starting " + getRuntimeContext().getTaskNameWithSubtasks() + " " + getTaskAllocationId() + " " + getJvmPid() + " " + configuration);
			ValueStateDescriptor<String> stateDescriptor =
				new ValueStateDescriptor<>("state", String.class);
			valueState = functionInitializationContext.getKeyedStateStore().getState(stateDescriptor);

			ListStateDescriptor<MapperTestInfo> mapperInfoStateDescriptor =
				new ListStateDescriptor<>("mapperState", MapperTestInfo.class);
			mapperState = functionInitializationContext.getOperatorStateStore().getUnionListState(mapperInfoStateDescriptor);

			if (functionInitializationContext.isRestored()) {
				Iterable<MapperTestInfo> iterable = mapperState.get();
				String taskNameWithSubtasks = getRuntimeContext().getTaskNameWithSubtasks();
				Set<Integer> killedJvmPids = new HashSet<>();
				MapperTestInfo infoForThisTask = null;
				if (iterable != null) {
					for (MapperTestInfo testInfo : iterable) {
						if (taskNameWithSubtasks.equals(testInfo.taskNameWithSubtask)) {
							infoForThisTask = testInfo;
						}

						if (testInfo.killedJvm) {
							killedJvmPids.add(testInfo.jvmPid);
						}
					}
				}

				signalTestFailure = !isScheduledToCorrectAllocation(infoForThisTask, killedJvmPids);

				if (signalTestFailure) {

					for (MapperTestInfo mapperTestInfo : iterable) {
						System.out.println(mapperTestInfo);
					}
					System.out.println(getRuntimeContext().getAttemptNumber() + "> NOW: " + getRuntimeContext().getTaskNameWithSubtasks() + " " + getTaskAllocationId() + " " + getJvmPid());

				}
			}
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			if (shouldTaskFailForCheckpoint(checkpointId)) {
				System.out.println(getRuntimeContext().getAttemptNumber() + ">Kill from task " + getJvmPid() + " " + getTaskAllocationId() + " on chk " + checkpointId);
				System.out.flush();
				failTask = true;
//				Runtime.getRuntime().halt(-1);
			}
		}

		private String getTaskAllocationId() {
			StreamingRuntimeContext streamingRuntimeContext = (StreamingRuntimeContext) getRuntimeContext();
			TaskStateManager taskStateManager = streamingRuntimeContext.getTaskEnvironment().getTaskStateManager();
			return String.valueOf(taskStateManager).replaceAll(ALLOCATION_ID_EXTRACTION_PATTERN, "$1");
		}

		private boolean shouldTaskFailForCheckpoint(long checkpointID) {
			RuntimeContext runtimeContext = getRuntimeContext();
			int numSubtasks = runtimeContext.getNumberOfParallelSubtasks();
			int subtaskIdx = runtimeContext.getIndexOfThisSubtask();
			return (checkpointID % numSubtasks) == subtaskIdx;
		}

		private boolean isScheduledToCorrectAllocation(MapperTestInfo infoForThisTask, Set<Integer> killedJvmPids) {
			return (infoForThisTask != null &&
				(infoForThisTask.allocationId.equals(getTaskAllocationId())
					|| killedJvmPids.contains(infoForThisTask.jvmPid)));
		}
	}

	private static final class NopMap extends RichMapFunction<String, String> implements CheckpointedFunction {

		public NopMap() {
		}

		private String getTaskAllocationId() {
			StreamingRuntimeContext streamingRuntimeContext = (StreamingRuntimeContext) getRuntimeContext();
			TaskStateManager taskStateManager = streamingRuntimeContext.getTaskEnvironment().getTaskStateManager();
			return String.valueOf(taskStateManager).replaceAll(ALLOCATION_ID_EXTRACTION_PATTERN, "$1");
		}

		@Override
		public String map(String value) throws Exception {
			return value;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {

		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			System.out.println(getRuntimeContext().getAttemptNumber() + "> Starting " + getRuntimeContext().getTaskNameWithSubtasks() + " " + getTaskAllocationId() + " " + getJvmPid());
		}
	}

	private static int getJvmPid() throws Exception {
		java.lang.management.RuntimeMXBean runtime =
			java.lang.management.ManagementFactory.getRuntimeMXBean();
		java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
		jvm.setAccessible(true);
		sun.management.VMManagement mgmt =
			(sun.management.VMManagement) jvm.get(runtime);
		java.lang.reflect.Method pidMethod =
			mgmt.getClass().getDeclaredMethod("getProcessId");
		pidMethod.setAccessible(true);

		return (int) (Integer) pidMethod.invoke(mgmt);
	}

	private static class MapperTestInfo implements Serializable {

		private static final long serialVersionUID = 1L;

		final boolean killedJvm;
		final int jvmPid;
		final String taskNameWithSubtask;
		final String allocationId;

		MapperTestInfo(boolean killedJvm, int jvmPid, String taskNameWithSubtask, String allocationId) {
			this.killedJvm = killedJvm;
			this.jvmPid = jvmPid;
			this.taskNameWithSubtask = taskNameWithSubtask;
			this.allocationId = allocationId;
		}

		@Override
		public String toString() {
			return "MapperTestInfo{" +
				"killedJvm=" + killedJvm +
				", jvmPid=" + jvmPid +
				", taskNameWithSubtask='" + taskNameWithSubtask + '\'' +
				", allocationId='" + allocationId + '\'' +
				'}';
		}
	}
}
