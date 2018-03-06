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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Simple test.
 */
public class SimpleStatefulJob {

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
		long numKeys = pt.has("numKeys") ? pt.getLong("numKeys") : 1_000_000L;
		int valueSize = pt.has("valueSize") ? pt.getInt("valueSize") : 10;
		int failAfter = pt.has("failAfter") ? pt.getInt("failAfter") : 0;

		env.addSource(new RandomLongSource(numKeys, delay))
			.setParallelism(1)
			.keyBy((KeySelector<Long, Long>) aLong -> aLong)
			.flatMap(new StateCreatingFlatMap(valueSize, failAfter))
			.setParallelism(parallelism)
			.map((MapFunction<Long, Object>) value -> value)
			.setParallelism(parallelism);

		env.execute("Streaming WordCount");
	}

	/**
	 * Source.
	 */
	private static final class RandomLongSource extends RichSourceFunction<Long> implements CheckpointedFunction {

		ListState<Long> state;
		final long numKeys;
		final long delay;
		volatile boolean running = true;
		long currentKey;

		RandomLongSource(long numKeys, long delay) {
			this.numKeys = numKeys;
			this.delay = delay;
		}

		@Override
		public void run(SourceContext<Long> sourceContext) throws Exception {

			while (currentKey < numKeys) {
				sourceContext.collect(currentKey);
				if (delay > 0) {
					Thread.sleep(delay);
				}
				++currentKey;
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
			ListStateDescriptor<Long> currentKeyDescriptor = new ListStateDescriptor<>("currentKey", Long.class);
			state = context.getOperatorStateStore().getListState(currentKeyDescriptor);
			Iterable<Long> iterable = state.get();
			currentKey = 0L;
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
		extends RichFlatMapFunction<Long, Long> implements CheckpointedFunction, CheckpointListener {

		private static final long serialVersionUID = 1L;

		final int failAfter;
		transient int count;
		volatile int chkCount;
		final int valueSize;

		transient ValueState<String> valueState;
		transient ListState<MapperTestInfo> mapperState;

		public StateCreatingFlatMap(int valueSize, int failAfter) {
			this.valueSize = valueSize;
			this.failAfter = failAfter;
			this.chkCount = 0;
		}

		@Override
		public void flatMap(Long key, Collector<Long> collector) throws IOException {
			if (count == 0) {
				LOG.info("started: " + System.nanoTime());
			}

			++count;

			if (failAfter != 0 && (count >= failAfter) && chkCount > 0) {
//				throw new RuntimeException("Artificial failure! " + System.nanoTime());
				if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
					System.out.println("==========================================");
					System.out.flush();
					System.exit(-1);
				}
			}

			if (null != valueState.value()) {
				throw new IllegalStateException("Should never happen!");
			}
			valueState.update(RandomStringUtils.random(valueSize, true, true));
		}

		@Override
		public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
			MapperTestInfo mapperTestInfo = new MapperTestInfo();
			mapperState.clear();
			mapperState.add(mapperTestInfo);
		}

		@Override
		public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
			StreamingRuntimeContext streamingRuntimeContext = (StreamingRuntimeContext) getRuntimeContext();
			String pattern = ".*allocationID=(.*?),.*";
			String basicInfo = "PID_" + getPID() + ": " + streamingRuntimeContext.getTaskNameWithSubtasks() + "_" + streamingRuntimeContext.getAttemptNumber();
			String allocationID = String.valueOf(streamingRuntimeContext.getTaskEnvironment().getTaskStateManager()).replaceAll(pattern, "$1");

			System.out.println(basicInfo + " with allocation id" + allocationID);

			ValueStateDescriptor<String> stateDescriptor =
				new ValueStateDescriptor<>("state", String.class);
			valueState = functionInitializationContext.getKeyedStateStore().getState(stateDescriptor);

			ListStateDescriptor<MapperTestInfo> mapperInfoStateDescriptor = new ListStateDescriptor<MapperTestInfo>("mapperState", MapperTestInfo.class);
			ListState<MapperTestInfo> mapperState = functionInitializationContext.getOperatorStateStore().getListState(mapperInfoStateDescriptor);

			this.count = 0;
			this.chkCount = 0;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			++chkCount;
		}
	}

	private static int getPID() throws Exception {
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

	private static class MapperTestInfo {
		boolean wasKilled;
		String allocationID;
	}
}
