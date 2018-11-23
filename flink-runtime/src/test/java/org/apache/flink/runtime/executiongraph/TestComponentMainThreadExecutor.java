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

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class TestComponentMainThreadExecutor extends ComponentMainThreadExecutorServiceAdapter {

	public TestComponentMainThreadExecutor(ScheduledExecutorService scheduledExecutorService, Thread mainThread) {
		super(scheduledExecutorService, () -> Thread.currentThread() == mainThread);
	}

	public static TestComponentMainThreadExecutor forMainThread() {
		final Thread main = Thread.currentThread();
		return new TestComponentMainThreadExecutor(new DirectScheduledExecutorService(), main);
	}

	public static TestComponentMainThreadExecutor forSingeThreadExecutor(ScheduledExecutorService singleThreadExecutor) {
		try {
			Thread thread = CompletableFuture.supplyAsync(Thread::currentThread, singleThreadExecutor).get();
			return new TestComponentMainThreadExecutor(singleThreadExecutor, thread);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
