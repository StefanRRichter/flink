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

package org.apache.flink.core.fs.local;

import java.io.Closeable;
import java.io.IOException;

/**
 * This is the client interface for a {@link org.apache.flink.core.fs.CloseableRegistry}.
 * This part of the interface is supposed to be visible for users to register objects that should be managed by a
 * registry.
 */
public interface CloseableRegistryClient {

	/**
	 * Registers a {@link Closeable} with the registry. In case the registry is already closed, this method throws an
	 * {@link IllegalStateException} and closes the passed {@link Closeable}.
	 *
	 * @param closeable Closeable tor register
	 * @throws IOException exception when the registry was closed before
	 */
	void registerCloseable(Closeable closeable) throws IOException;

	/**
	 * Removes a {@link Closeable} from the registry.
	 *
	 * @param closeable instance to remove from the registry.
	 * @return true if the closeable was previously registered and became unregistered through this call.
	 */
	boolean unregisterCloseable(Closeable closeable);

	/**
	 * True iff the registry is closed.
	 */
	boolean isClosed();
}
