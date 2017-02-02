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

import org.apache.flink.util.Preconditions;

public class KeyNamespace<K, N> {

	private final K key;
	private final N namespace;
	private final int cachedHash;
	private int version;

	public KeyNamespace(K key, N namespace, int version) {
		this.key = Preconditions.checkNotNull(key);
		this.namespace = Preconditions.checkNotNull(namespace);
		this.version = version;
		this.cachedHash  = 31 * key.hashCode() + namespace.hashCode();
	}

	public N getNamespace() {
		return namespace;
	}

	public K getKey() {
		return key;
	}

	public int getCachedHash() {
		return cachedHash;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public void incrementVersion() {
		++this.version;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		KeyNamespace<?, ?> that = (KeyNamespace<?, ?>) o;
		return (cachedHash == that.cachedHash) && key.equals(that.key) && namespace.equals(that.namespace);
	}

	@Override
	public int hashCode() {
		return cachedHash;
	}
}