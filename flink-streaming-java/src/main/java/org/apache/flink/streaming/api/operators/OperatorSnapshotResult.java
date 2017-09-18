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

import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.snapshot.KeyedStateSnapshot;
import org.apache.flink.runtime.state.snapshot.OperatorStateSnapshot;
import org.apache.flink.util.ExceptionUtils;

import java.util.Collection;
import java.util.concurrent.RunnableFuture;

/**
 * Result of {@link StreamOperator#snapshotState}.
 */
public class OperatorSnapshotResult {

	private RunnableFuture<Collection<KeyedStateSnapshot>> keyedStateManagedFuture;
	private RunnableFuture<Collection<KeyedStateSnapshot>> keyedStateRawFuture;
	private RunnableFuture<Collection<OperatorStateSnapshot>> operatorStateManagedFuture;
	private RunnableFuture<Collection<OperatorStateSnapshot>> operatorStateRawFuture;

	public OperatorSnapshotResult() {
		this(null, null, null, null);
	}

	public OperatorSnapshotResult(
			RunnableFuture<Collection<KeyedStateSnapshot>> keyedStateManagedFuture,
			RunnableFuture<Collection<KeyedStateSnapshot>> keyedStateRawFuture,
			RunnableFuture<Collection<OperatorStateSnapshot>> operatorStateManagedFuture,
			RunnableFuture<Collection<OperatorStateSnapshot>> operatorStateRawFuture) {
		this.keyedStateManagedFuture = keyedStateManagedFuture;
		this.keyedStateRawFuture = keyedStateRawFuture;
		this.operatorStateManagedFuture = operatorStateManagedFuture;
		this.operatorStateRawFuture = operatorStateRawFuture;
	}

	public RunnableFuture<Collection<KeyedStateSnapshot>> getKeyedStateManagedFuture() {
		return keyedStateManagedFuture;
	}

	public void setKeyedStateManagedFuture(RunnableFuture<Collection<KeyedStateSnapshot>> keyedStateManagedFuture) {
		this.keyedStateManagedFuture = keyedStateManagedFuture;
	}

	public RunnableFuture<Collection<KeyedStateSnapshot>> getKeyedStateRawFuture() {
		return keyedStateRawFuture;
	}

	public void setKeyedStateRawFuture(RunnableFuture<Collection<KeyedStateSnapshot>> keyedStateRawFuture) {
		this.keyedStateRawFuture = keyedStateRawFuture;
	}

	public RunnableFuture<Collection<OperatorStateSnapshot>> getOperatorStateManagedFuture() {
		return operatorStateManagedFuture;
	}

	public void setOperatorStateManagedFuture(RunnableFuture<Collection<OperatorStateSnapshot>> operatorStateManagedFuture) {
		this.operatorStateManagedFuture = operatorStateManagedFuture;
	}

	public RunnableFuture<Collection<OperatorStateSnapshot>> getOperatorStateRawFuture() {
		return operatorStateRawFuture;
	}

	public void setOperatorStateRawFuture(RunnableFuture<Collection<OperatorStateSnapshot>> operatorStateRawFuture) {
		this.operatorStateRawFuture = operatorStateRawFuture;
	}

	public void cancel() throws Exception {
		Exception exception = null;

		try {
			StateUtil.discardStateCollectionFuture(getKeyedStateManagedFuture());
		} catch (Exception e) {
			exception = new Exception("Could not properly cancel managed keyed state future.", e);
		}

		try {
			StateUtil.discardStateCollectionFuture(getOperatorStateManagedFuture());
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(
				new Exception("Could not properly cancel managed operator state future.", e),
				exception);
		}

		try {
			StateUtil.discardStateCollectionFuture(getKeyedStateRawFuture());
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(
				new Exception("Could not properly cancel raw keyed state future.", e),
				exception);
		}

		try {
			StateUtil.discardStateCollectionFuture(getOperatorStateRawFuture());
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(
				new Exception("Could not properly cancel raw operator state future.", e),
				exception);
		}

		if (exception != null) {
			throw exception;
		}
	}
}
