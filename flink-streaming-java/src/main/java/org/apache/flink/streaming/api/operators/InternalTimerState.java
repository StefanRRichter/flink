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

import org.apache.flink.annotation.Internal;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;

/**
 * Interface for internal timer state.
 *
 * @param <K> type of the timer's key.
 * @param <N> type of the timer's namespace.
 */
@Internal
public interface InternalTimerState<K, N> {

	/**
	 * Retrieves and removes the first timer (w.r.t. timestamps) of this state,
	 * or returns {@code null} if this state is empty.
	 *
	 * @return the first timer of this state, or {@code null} if this state is empty.
	 */
	@Nullable
	InternalTimer<K, N> poll();

	/**
	 * Retrieves, but does not remove, the first timer (w.r.t. timestamps) of this state,
	 * or returns {@code null} if this state is empty.
	 *
	 * @return the first timer (w.r.t. timestamps) of this state, or {@code null} if this state is empty.
	 */
	@Nullable
	InternalTimer<K, N> peek();

	/**
	 * Adds a new timer with the given timestamp, key, and namespace to the state, if an identical timer was not yet
	 * registered.
	 *
	 * @param timestamp the timer timestamp.
	 * @param key the timer key.
	 * @param namespace the timer namespace.
	 * @return true iff a new timer with given timestamp, key, and namespace was added to the state.
	 */
	boolean scheduleTimer(long timestamp, @Nonnull K key, @Nonnull N namespace);

	/**
	 * Stops timer with the given timestamp, key, and namespace by removing it from the state, if it exists in the state.
	 *
	 * @param timestamp the timer timestamp.
	 * @param key the timer key.
	 * @param namespace the timer namespace.
	 * @return true iff a timer with given timestamp, key, and namespace was found and removed from the state.
	 */
	boolean stopTimer(long timestamp, @Nonnull K key, @Nonnull N namespace);

	/**
	 * Check if the state contains any timers.
	 *
	 * @return true if the state is empty, i.e. no timer is scheduled.
	 */
	boolean isEmpty();

	/**
	 * Returns the number of scheduled timer in this state.
	 *
	 * @return the number of currently scheduled timers in this state.
	 */
	@Nonnegative
	int size();

	/**
	 * Stops all timers currently registered in the state.
	 */
	void clear();

	/**
	 * Adds all the given timers to the state.
	 */
	void addAll(@Nullable Collection<? extends InternalTimer<K, N>> restoredTimers);
}
