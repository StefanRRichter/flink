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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;

public abstract class AbstractStateTable<K, N, S> implements Iterable<StateEntry<K, N, S>> {

	/**
	 * Combined meta information such as name and serializers for this state
	 */
	private RegisteredBackendStateMetaInfo<N, S> metaInfo;

	/**
	 * Constructs a new {@code StateTable} with default capacity of 128.
	 *
	 * @param metaInfo the meta information, including the type serializer for state copy-on-write.
	 */
	public AbstractStateTable(RegisteredBackendStateMetaInfo<N, S> metaInfo) {
		this.metaInfo = Preconditions.checkNotNull(metaInfo);
	}

	// Main interface methods of StateTable ----------------------------------------------------------------------------

	/**
	 * Returns whether this {@link StateTable} is empty.
	 *
	 * @return {@code true} if this {@link StateTable} has no elements, {@code false}
	 * otherwise.
	 * @see #size()
	 */
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Returns the total number of entries in this {@link StateTable}. This is the sum of both sub-tables.
	 *
	 * @return the number of entries in this {@link StateTable}.
	 */
	public abstract int size();

	/**
	 * Returns the value of the mapping with the specified key/namespace composite key.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @return the value of the mapping with the specified key/namespace composite key, or {@code null}
	 * if no mapping for the specified key is found.
	 */
	public abstract S get(Object key, Object namespace);

	/**
	 * Returns whether this table contains the specified key/namespace composite key.
	 *
	 * @param key       the key in the composite key to search for. Not null.
	 * @param namespace the namespace in the composite key to search for. Not null.
	 * @return {@code true} if this map contains the specified key/namespace composite key,
	 * {@code false} otherwise.
	 */
	public abstract boolean containsKey(Object key, Object namespace);

	/**
	 * Maps the specified key/namespace composite key to the specified value. This method should be preferred
	 * over {@link #putAndGetOld(Object, Object, Object)} (Object, Object)} when the caller is not interested
	 * in the old value, because this can potentially reduce copy-on-write activity.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @param value     the value. Can be null.
	 */
	public abstract void put(K key, N namespace, S value);

	/**
	 * Maps the specified key/namespace composite key to the specified value. Returns the previous state that was
	 * registered under the composite key.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @param value     the value. Can be null.
	 * @return the value of any previous mapping with the specified key or
	 * {@code null} if there was no such mapping.
	 */
	public abstract S putAndGetOld(K key, N namespace, S value);

	/**
	 * Removes the mapping with the specified key/namespace composite key from this map. This method should be preferred
	 * over {@link #removeAndGetOld(Object, Object)} when the caller is not interested in the old value, because this
	 * can potentially reduce copy-on-write activity.
	 *
	 * @param key       the key of the mapping to remove. Not null.
	 * @param namespace the namespace of the mapping to remove. Not null.
	 */
	public abstract void remove(Object key, Object namespace);

	/**
	 * Removes the mapping with the specified key/namespace composite key from this map, returning the state that was
	 * found under the entry.
	 *
	 * @param key       the key of the mapping to remove. Not null.
	 * @param namespace the namespace of the mapping to remove. Not null.
	 * @return the value of the removed mapping or {@code null} if no mapping
	 * for the specified key was found.
	 */
	public abstract S removeAndGetOld(Object key, Object namespace);

	// Meta data setter / getter and toString --------------------------------------------------------------------------

	public TypeSerializer<S> getStateSerializer() {
		return metaInfo.getStateSerializer();
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return metaInfo.getNamespaceSerializer();
	}

	public RegisteredBackendStateMetaInfo<N, S> getMetaInfo() {
		return metaInfo;
	}

	public void setMetaInfo(RegisteredBackendStateMetaInfo<N, S> metaInfo) {
		this.metaInfo = metaInfo;
	}

	// Iteration  ------------------------------------------------------------------------------------------------------

	@Override
	public abstract Iterator<StateEntry<K, N, S>> iterator();




}