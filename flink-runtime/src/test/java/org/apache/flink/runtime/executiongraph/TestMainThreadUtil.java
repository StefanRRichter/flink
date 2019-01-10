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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;

public class TestMainThreadUtil {

	@Nonnull
	private final TestComponentMainThreadExecutor mainThreadExecutor;

	public TestMainThreadUtil() {
		this(TestComponentMainThreadExecutor.forSingeThreadExecutor(Executors.newSingleThreadScheduledExecutor()));
	}

	public TestMainThreadUtil(@Nonnull TestComponentMainThreadExecutor mainThreadExecutor) {
		this.mainThreadExecutor = mainThreadExecutor;
	}

	public <U> U execute(@Nonnull SupplierWithException<U, Throwable> supplierWithException) {
		try {
			return CompletableFuture.supplyAsync(() -> {
				try {
					return supplierWithException.get();
				} catch (Throwable e) {
					throw new CompletionException(e);
				}
			}, mainThreadExecutor).get();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void execute(@Nonnull ThrowingRunnable<Throwable> throwingRunnable) {
		execute(() -> {
			throwingRunnable.run();
			return null;
		});
	}

	@Nonnull
	public TestComponentMainThreadExecutor getMainThreadExecutor() {
		return mainThreadExecutor;
	}
}
