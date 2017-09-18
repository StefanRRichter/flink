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

package org.apache.flink.runtime.state.snapshot;

import org.apache.flink.runtime.state.StateObject;

import javax.annotation.Nullable;

import java.util.Collection;

public class OperatorSubtaskStateReport {

	/**
	 * Snapshot from the {@link org.apache.flink.runtime.state.OperatorStateBackend}.
	 */
	@Nullable
	private final Collection<OperatorStateHandleSnapshot> managedOperatorState;

	/**
	 * Snapshot written using {@link org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream}.
	 */
	@Nullable
	private final Collection<OperatorStateHandleSnapshot> rawOperatorState;

	/**
	 * Snapshot from {@link org.apache.flink.runtime.state.KeyedStateBackend}.
	 */
	@Nullable
	private final Collection<KeyedStateHandleSnapshot> managedKeyedState;

	/**
	 * Snapshot written using {@link org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream}.
	 */
	@Nullable
	private final Collection<KeyedStateHandleSnapshot> rawKeyedState;

	/**
	 * Empty state.
	 */
	public OperatorSubtaskStateReport() {
		this(
			null,
			null,
			null,
			null);
	}

	public OperatorSubtaskStateReport(
		Collection<OperatorStateHandleSnapshot> managedOperatorState,
		Collection<OperatorStateHandleSnapshot> rawOperatorState,
		Collection<KeyedStateHandleSnapshot> managedKeyedState,
		Collection<KeyedStateHandleSnapshot> rawKeyedState) {

		this.managedOperatorState = managedOperatorState;
		this.rawOperatorState = rawOperatorState;
		this.managedKeyedState = managedKeyedState;
		this.rawKeyedState = rawKeyedState;
	}

	// --------------------------------------------------------------------------------------------


	@Nullable
	public Collection<OperatorStateHandleSnapshot> getManagedOperatorState() {
		return managedOperatorState;
	}

	@Nullable
	public Collection<OperatorStateHandleSnapshot> getRawOperatorState() {
		return rawOperatorState;
	}

	@Nullable
	public Collection<KeyedStateHandleSnapshot> getManagedKeyedState() {
		return managedKeyedState;
	}

	@Nullable
	public Collection<KeyedStateHandleSnapshot> getRawKeyedState() {
		return rawKeyedState;
	}

	public boolean hasState() {
		return hasState(managedOperatorState)
			|| hasState(rawOperatorState)
			|| hasState(managedKeyedState)
			|| hasState(rawKeyedState);
	}

	private boolean hasState(StateObject state) {
		return state != null;
	}

	private boolean hasState(Iterable<? extends StateObject> states) {

		if (states == null) {
			return false;
		}

		for (StateObject state : states) {
			if (state != null) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String toString() {
		return "OperatorSubtaskStateReport{" +
			"managedOperatorState=" + managedOperatorState +
			", rawOperatorState=" + rawOperatorState +
			", managedKeyedState=" + managedKeyedState +
			", rawKeyedState=" + rawKeyedState +
			'}';
	}
}
