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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.state.ManualWindowSpeedITCase;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class ExternalizedCheckpointITCase {

	private static final int numTaskManagers = 2;
	private static final int slotsPerTaskManager = 2;

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testExternalizedIncrementalCheckpoints() throws Exception {

		final Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, slotsPerTaskManager);

		final File checkpointDir = temporaryFolder.newFolder();
		final File savepointDir = temporaryFolder.newFolder();

		config.setString("state.checkpoints.dir", checkpointDir.toURI().toString());
		config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY, checkpointDir.toURI().toString());
		config.setString(CoreOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

		String recoverPath = null;

		TestingCluster cluster = new TestingCluster(config);
		cluster.start();
		try {
			for (int i = 0; i < 3; ++i) {
				final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

				env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
				env.setParallelism(2);
				env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

				env.setStateBackend(new RocksDBStateBackend(checkpointDir.toURI().toString(), true));

				env.addSource(new ManualWindowSpeedITCase.InfiniteTupleSource(10_000))
					.keyBy(0)
					.timeWindow(Time.seconds(3))
					.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<String, Integer> reduce(
							Tuple2<String, Integer> value1,
							Tuple2<String, Integer> value2) throws Exception {
							return Tuple2.of(value1.f0, value1.f1 + value2.f1);
						}
					})
					.filter(new FilterFunction<Tuple2<String, Integer>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public boolean filter(Tuple2<String, Integer> value) throws Exception {
							return value.f0.startsWith("Tuple 0");
						}
					});

				StreamGraph streamGraph = env.getStreamGraph();
				streamGraph.setJobName("Test");

				JobGraph jobGraph = streamGraph.getJobGraph();
				if(recoverPath != null) {
					jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(recoverPath));
				}

				config.addAll(jobGraph.getJobConfiguration());
				JobSubmissionResult submissionResult = cluster.submitJobDetached(jobGraph);

				Thread.sleep(2000);
				recoverPath =
					cluster.requestCheckpoint(submissionResult.getJobID(), CheckpointOptions.forFullCheckpoint());
				System.out.println(recoverPath);
				cluster.cancelJob(submissionResult.getJobID());
			}
		} finally {
			cluster.stop();
			cluster.awaitTermination();
		}
	}

	static class Source implements SourceFunction<Integer> {

		private static final long serialVersionUID = 1L;
		int value = 0;
		volatile boolean running = true;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				System.out.println(value);
				ctx.collect(value);
				++value;
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
