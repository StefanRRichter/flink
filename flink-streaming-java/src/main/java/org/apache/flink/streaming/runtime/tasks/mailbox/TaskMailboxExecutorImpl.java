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
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;

public class TaskMailboxExecutorImpl implements TaskMailboxExecutor, Mailbox.DroppedLetterHandler {

	private final Thread taskMainThread;
	private final Mailbox mailbox;

	public TaskMailboxExecutorImpl(Mailbox mailbox) {
		this(mailbox, Thread.currentThread());
	}

	private TaskMailboxExecutorImpl(Mailbox mailbox, Thread taskMainThread) {
		this.mailbox = mailbox;
		this.taskMainThread = taskMainThread;
	}

	@Override
	public void execute(@Nonnull Runnable command) throws RejectedExecutionException {
		assert (taskMainThread != Thread.currentThread());
		try {
			mailbox.putMail(command);
		} catch (InterruptedException ire) {
			Thread.currentThread().interrupt();
			throw new RejectedExecutionException("Sender thread was interrupted while blocking on mailbox.", ire);
		}
	}

	@Override
	public boolean tryExecute(Runnable command) throws RejectedExecutionException {
		return mailbox.tryPutMail(command);
	}

	@Override
	public <T> CompletableFuture<T> executeSupplier(@Nonnull Supplier<T> supplier) {
		final CompletableFuture<T> resultOut = new CompletableFuture<>();

		try {
			execute(new CancelableRunnable<>(supplier, resultOut));
		} catch (Exception ex) {
			resultOut.completeExceptionally(ex);
		}

		return resultOut;
	}

	@Override
	public <T> Optional<CompletableFuture<T>> tryExecuteSupplier(@Nonnull Supplier<T> supplier) {

		final CompletableFuture<T> resultOut = new CompletableFuture<>();

		try {
			if (!tryExecute(new CancelableRunnable<>(supplier, resultOut))) {
				return Optional.empty();
			}
		} catch (Exception ex) {
			resultOut.completeExceptionally(ex);
		}

		return Optional.of(resultOut);
	}

	@Override
	public void waitUntilCanExecute() throws InterruptedException {
		assert (taskMainThread != Thread.currentThread());
		mailbox.waitUntilHasCapacity();
	}

	@Override
	public void yield() throws InterruptedException {
		assert (taskMainThread == Thread.currentThread());
		mailbox.takeMail().run();
	}

	@Override
	public boolean tryYield() {
		assert (taskMainThread == Thread.currentThread());
		Optional<Runnable> runnableOptional = mailbox.tryTakeMail();

		if (!runnableOptional.isPresent()) {
			return false;
		}

		runnableOptional.get().run();
		return true;
	}

	@Override
	public void handle(@Nonnull Runnable droppedLetter) {
		if (droppedLetter instanceof CancelableRunnable) {
			((CancelableRunnable<?>) droppedLetter).cancel(
				new RejectedExecutionException("Letter was dropped from the mailbox."));
		}
	}

	/**
	 * Runnable with the option to cancel the {@link CompletableFuture} that is connected to the execution result.
	 *
	 * @param <T> type of the supplier.
	 */
	final static class CancelableRunnable<T> implements Runnable {

		/** {@link CompletableFuture} that is used to output the result of invoking the supplier. */
		final CompletableFuture<T> resultOutputFuture;

		/** Supplier to invoke in {@link #run()}*/
		final Supplier<T> supplier;

		CancelableRunnable(Supplier<T> supplier, CompletableFuture<T> resultOutput) {
			this.supplier = supplier;
			this.resultOutputFuture = resultOutput;
		}

		@Override
		public void run() {
			try {
				resultOutputFuture.complete(supplier.get());
			} catch (Exception e) {
				resultOutputFuture.completeExceptionally(e);
			}
		}

		public void cancel(Exception reason) {
			resultOutputFuture.completeExceptionally(reason);
		}
	}
}
