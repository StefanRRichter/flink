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

package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.core.fs.CloseableRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @param <T>
 */
public abstract class AsyncSnapshotCallable<T> implements Callable<T> {

	private static final String CANCELLATION_EXCEPTION_MSG = "Snapshot was cancelled.";
	protected static final Logger LOG = LoggerFactory.getLogger(AsyncSnapshotCallable.class);

	@Nonnull
	private final AtomicBoolean resourceCleanupOwnershipTaken;

	@Nonnull
	private final CloseableRegistry taskCancelCloseableRegistry;

	@Nonnull
	protected final CloseableRegistry snapshotCloseableRegistry;

	protected AsyncSnapshotCallable(@Nonnull CloseableRegistry taskCancelCloseableRegistry) {
		this.taskCancelCloseableRegistry = taskCancelCloseableRegistry;
		this.snapshotCloseableRegistry = new CloseableRegistry();
		this.resourceCleanupOwnershipTaken = new AtomicBoolean(false);
	}

	@Override
	public T call() throws Exception {
		long startTime = System.currentTimeMillis();

		if (resourceCleanupOwnershipTaken.compareAndSet(false, true)) {

			try {

				taskCancelCloseableRegistry.registerCloseable(snapshotCloseableRegistry);

				final T result = callInternal();

				LOG.info("Asynchronous RocksDB snapshot (asynchronous part) in thread {} took {} ms.",
					Thread.currentThread(), (System.currentTimeMillis() - startTime));

				return result;
			} catch (Exception ex) {
				if (!snapshotCloseableRegistry.isClosed()) {
					throw ex;
				}
			} finally {
				closeSnapshotIO();
				cleanup();
			}
		}

		throw new CancellationException(CANCELLATION_EXCEPTION_MSG);
	}

	protected abstract T callInternal() throws Exception;

	protected abstract void cleanupProvidedResources();

	private void cleanup() {
		taskCancelCloseableRegistry.unregisterCloseable(snapshotCloseableRegistry);
		cleanupProvidedResources();
	}

	public void cancel() {
		closeSnapshotIO();
		if (resourceCleanupOwnershipTaken.compareAndSet(false, true)) {
			cleanup();
		}
	}


	private void closeSnapshotIO() {
		try {
			snapshotCloseableRegistry.close();
		} catch (IOException e) {
			LOG.warn("Could not properly close incremental snapshot streams.", e);
		}
	}
}
