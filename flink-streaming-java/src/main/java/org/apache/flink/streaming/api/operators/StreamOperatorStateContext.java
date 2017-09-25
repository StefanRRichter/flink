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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;

import java.io.IOException;

/**
 * This interface represents a context from which a stream operator can initialize everything related to state.
 */
public interface StreamOperatorStateContext {

	/**
	 * Returns true, the states provided by this context are restored from a checkpoint/savepoint.
	 */
	boolean isRestored();

	/**
	 * Returns the operator state backend for the stream operator.
	 */
	OperatorStateBackend operatorStateBackend() throws Exception;


	/**
	 * Returns the keyed state backend for the stream operator. This method returns null for non-keyed operators.
	 */
	AbstractKeyedStateBackend<?> keyedStateBackend() throws Exception;

	/**
	 * Returns the internal timer service manager for the stream operator. This method returns null for non-keyed
	 * operators.
	 */
	InternalTimeServiceManager<?, ?> internalTimerServiceManager() throws Exception;

	/**
	 * Returns the checkpoint stream factory for the stream operator.
	 */
	CheckpointStreamFactory checkpointStreamFactory() throws IOException;

	/**
	 * Returns an iterable to obtain input streams for previously stored operator state partitions that are assigned to
	 * this stream operator.
	 */
	Iterable<StatePartitionStreamProvider> rawOperatorStateInputs();

	/**
	 * Returns an iterable to obtain input streams for previously stored keyed state partitions that are assigned to
	 * this operator. This method returns null for non-keyed operators.
	 */
	Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs();

}
