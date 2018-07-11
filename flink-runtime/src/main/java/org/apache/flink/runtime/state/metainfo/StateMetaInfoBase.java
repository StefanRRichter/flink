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

package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.api.common.state.StateDescriptor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * Base class for {@link StateMetaInfo} and {@link StateMetaInfo.Snapshot}. Contains the state name and the options.
 */
public abstract class StateMetaInfoBase {

	/**
	 * Predefined keys for the most common options in the meta info.
	 */
	public enum CommonOptionsKeys {
		/** Key to define the {@link StateDescriptor.Type} of a key/value keyed-state */
		KEYED_STATE_TYPE,
		/**
		 * Key to define {@link org.apache.flink.runtime.state.OperatorStateHandle.Mode}, about how operator state is
		 * distributed on restore
		 */
		OPERATOR_STATE_DISTRIBUTION_MODE,
	}

	/** The name of the state. */
	@Nonnull
	protected final String name;

	/** Map of options (encoded as strings) for the state. */
	@Nonnull
	protected final Map<String, String> options;

	protected StateMetaInfoBase(@Nonnull String name, @Nonnull Map<String, String> options) {
		this.name = name;
		this.options = options;
	}

	@Nullable
	public String getOption(@Nonnull String key) {
		return options.get(key);
	}

	public void setOption(@Nonnull String key, @Nullable String option) {
		if (option != null) {
			options.put(key, option);
		} else {
			options.remove(key);
		}
	}

	@Nullable
	public String getOption(@Nonnull CommonOptionsKeys key) {
		return getOption(key.toString());
	}

	public void setOption(@Nonnull CommonOptionsKeys key, @Nullable String option) {
		setOption(key.toString(), option);
	}

	@Nonnull
	public Map<String, String> getOptionsImmutable() {
		return Collections.unmodifiableMap(options);
	}

	@Nonnull
	public String getName() {
		return name;
	}

	@Override
	public abstract int hashCode();

	@Override
	public abstract boolean equals(Object obj);
}
