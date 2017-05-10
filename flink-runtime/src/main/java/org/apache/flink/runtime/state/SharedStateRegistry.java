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

package org.apache.flink.runtime.state;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@code SharedStateRegistry} will be deployed in the 
 * {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator} to 
 * maintain the reference count of {@link SharedStateHandle}s which are shared
 * among different checkpoints.
 *
 */
public class SharedStateRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(SharedStateRegistry.class);

	/** All registered state objects by an artificial key */
	private final Map<SharedStateRegistryKey, SharedStateRegistry.SharedStateEntry> registeredStates;

	public SharedStateRegistry() {
		this.registeredStates = new HashMap<>();
	}

	/**
	 * Register a reference to the given shared state in the registry. This increases the reference
	 * count for the this shared state by one. Returns the reference count after the update.
	 *
	 * @param state the shared state for which we register a reference.
	 * @return the updated reference count for the given shared state.
	 */
	public StreamStateHandle tryRegisterNew(SharedStateRegistryKey registrationKey, StreamStateHandle state) {
		Preconditions.checkNotNull(state);

		synchronized (registeredStates) {
			SharedStateRegistry.SharedStateEntry entry =
				registeredStates.get(registrationKey);

			if (entry == null) {
				SharedStateRegistry.SharedStateEntry stateEntry =
					new SharedStateRegistry.SharedStateEntry(state);
				registeredStates.put(registrationKey, stateEntry);
				return state;
			} else {
				entry.increaseReferenceCount();
				return entry.getState();
			}
		}
	}

	public int increaseReferenceCount(SharedStateRegistryKey registrationKey) {
		Preconditions.checkNotNull(registrationKey);

		synchronized (registeredStates) {
			SharedStateRegistry.SharedStateEntry entry =
				Preconditions.checkNotNull(registeredStates.get(registrationKey),
					"Could not find a state for the given registration key!");
			entry.increaseReferenceCount();
			return entry.getReferenceCount();
		}
	}

	/**
	 * Unregister one reference to the given shared state in the registry. This decreases the
	 * reference count by one. Once the count reaches zero, the shared state is deleted.
	 *
	 * @param registrationKey the shared state for which we unregister a reference.
	 * @return the reference count for the shared state after the update.
	 */
	public int decreaseReferenceCount(SharedStateRegistryKey registrationKey) {
		Preconditions.checkNotNull(registrationKey);

		synchronized (registeredStates) {
			SharedStateRegistry.SharedStateEntry entry = registeredStates.get(registrationKey);

			Preconditions.checkState(entry != null,
				"Cannot unregister a state that is not registered.");

			entry.decreaseReferenceCount();

			final int newReferenceCount = entry.getReferenceCount();

			// Remove the state from the registry when it's not referenced any more.
			if (newReferenceCount <= 0) {
				registeredStates.remove(registrationKey);
				try {
					entry.getState().discardState(); //TODO async!
				} catch (Exception e) {
					LOG.warn("Cannot properly discard the state {}.", entry.getState(), e);
				}
			}
			return newReferenceCount;
		}
	}

	/**
	 * Register given shared states in the registry.
	 *
	 * @param stateHandles The shared states to register.
	 */
	public void registerAll(Iterable<? extends CompositeStateHandle> stateHandles) {
		if (stateHandles == null) {
			return;
		}

		synchronized (registeredStates) {
			for (CompositeStateHandle stateHandle : stateHandles) {
				stateHandle.registerSharedStates(this);
			}
		}
	}



	/**
	 * Unregister all the shared states referenced by the given.
	 *
	 * @param stateHandles The shared states to unregister.
	 */
	public void unregisterAll(Iterable<? extends CompositeStateHandle> stateHandles) {
		if (stateHandles == null) {
			return;
		}

		synchronized (registeredStates) {
			for (CompositeStateHandle stateHandle : stateHandles) {
				stateHandle.unregisterSharedStates(this);
			}
		}
	}

	private static class SharedStateEntry {

		/** The shared object */
		private final StreamStateHandle state;

		/** The reference count of the object */
		private int referenceCount;

		SharedStateEntry(StreamStateHandle value) {
			this.state = value;
			this.referenceCount = 1;
		}

		StreamStateHandle getState() {
			return state;
		}

		int getReferenceCount() {
			return referenceCount;
		}

		void increaseReferenceCount() {
			++referenceCount;
		}

		void decreaseReferenceCount() {
			--referenceCount;
		}
	}

	public static class Result {
		private final StreamStateHandle reference;
		private final int referenceCount;

		public Result(StreamStateHandle reference, int referenceCount) {
			this.reference = reference;
			this.referenceCount = referenceCount;
		}

		public StreamStateHandle getReference() {
			return reference;
		}

		public int getReferenceCount() {
			return referenceCount;
		}
	}
}
