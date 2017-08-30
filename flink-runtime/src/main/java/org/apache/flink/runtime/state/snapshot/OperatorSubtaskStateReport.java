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

import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateObject;

import javax.annotation.Nullable;

import java.util.Collection;

public class OperatorSubtaskStateReport {

	/**
	 * Snapshot from the {@link org.apache.flink.runtime.state.OperatorStateBackend}.
	 */
	@Nullable
	private final OperatorStateHandle managedOperatorState;

	/**
	 * Snapshot written using {@link org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream}.
	 */
	@Nullable
	private final OperatorStateHandle rawOperatorState;

	/**
	 * Snapshot from {@link org.apache.flink.runtime.state.KeyedStateBackend}.
	 */
	@Nullable
	private final Collection<KeyedStateSnapshot> managedKeyedState;

	/**
	 * Snapshot written using {@link org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream}.
	 */
	@Nullable
	private final KeyedStateHandle rawKeyedState;

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
		OperatorStateHandle managedOperatorState,
		OperatorStateHandle rawOperatorState,
		Collection<KeyedStateSnapshot> managedKeyedState,
		KeyedStateHandle rawKeyedState) {

		this.managedOperatorState = managedOperatorState;
		this.rawOperatorState = rawOperatorState;
		this.managedKeyedState = managedKeyedState;
		this.rawKeyedState = rawKeyedState;
	}

	// --------------------------------------------------------------------------------------------


	@Nullable
	public OperatorStateHandle getManagedOperatorState() {
		return managedOperatorState;
	}

	@Nullable
	public OperatorStateHandle getRawOperatorState() {
		return rawOperatorState;
	}

	@Nullable
	public Collection<KeyedStateSnapshot> getManagedKeyedState() {
		return managedKeyedState;
	}

	@Nullable
	public KeyedStateHandle getRawKeyedState() {
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
