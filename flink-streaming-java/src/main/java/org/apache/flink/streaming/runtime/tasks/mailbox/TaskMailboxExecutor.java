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

package org.apache.flink.streaming.runtime.tasks.mailbox;

import javax.annotation.Nonnull;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;

/**
 * TODO.
 */
public interface TaskMailboxExecutor extends Executor {

	/**
	 * Executes the given command at some time in the future in the mailbox thread. Unlike many other executors,
	 * this call can block when the mailbox is currently full. This method must not be called from the mailbox
	 * thread itself as this can cause a deadlock. Instead, if the caller is already in the mailbox thread, the command
	 * should just be executed directly.
	 *
	 * @param command the runnable task.
	 * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g. because the mailbox is
	 * closed.
	 */
	@Override
	void execute(@Nonnull Runnable command) throws RejectedExecutionException;

	/**
	 *
	 * @param command
	 * @return true if the command was added to the mailbox. False if the command could not be added because the mailbox was full.
	 * @throws RejectedExecutionException
	 */
	boolean tryExecute(Runnable command) throws RejectedExecutionException;

	<T> CompletableFuture<T> executeSupplier(@Nonnull Supplier<T> supplier);

	<T> Optional<CompletableFuture<T>> tryExecuteSupplier(@Nonnull Supplier<T> supplier);

	/**
	 * This method blocks until the mailbox potentially has again capacity to put a command.
	 *
	 * @throws InterruptedException on interruption.
	 */
	void waitUntilCanExecute() throws InterruptedException;

	/**
	 *
	 * @throws InterruptedException
	 */
	void yield() throws InterruptedException;

	/**
	 *
	 * @return
	 */
	boolean tryYield();
}
