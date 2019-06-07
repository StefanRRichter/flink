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

import java.util.concurrent.RejectedExecutionException;

/**
 * A mailbox is basically a blocking queue for inter-thread message exchange in form of {@link Runnable} objects between
 * multiple producer threads and a single consumer.
 */
public interface Mailbox extends MailboxReceiver, MailboxSender, AutoCloseable {

	/**
	 * Open the mailbox. In this state, the mailbox supports put and take operations.
	 */
	void open();

	/**
	 * Quiesce the mailbox. In this state, the mailbox supports only take operations and all pending and future put
	 * operations will throw {@link java.util.concurrent.RejectedExecutionException}.
	 */
	void quiesce();

	/**
	 * Close the mailbox. In this state, all pending and future put operations will throw
	 * {@link java.util.concurrent.RejectedExecutionException} and all pending and future take operations will throw
	 * {@link IllegalStateException}.
	 */
	@Override
	void close();

	/**
	 * The effect of this is that all pending letters in the mailbox are dropped and the given priorityAction
	 * is enqueued to the head of the mailbox. Dropped letters will be passed into the given
	 * {@link DroppedLetterHandler}. This method should only be invoked by code that has ownership of the mailbox
	 * object and only rarely used, e.g. to submit special events like shutting down the mailbox loop.
	 *
	 * @param priorityAction action to enqueue atomically after the mailbox was cleared.
	 * @throws RejectedExecutionException if the mailbox is quiesced or closed.
	 */
	void clearAndPut(
		@Nonnull Runnable priorityAction,
		@Nonnull DroppedLetterHandler droppedLetterHandler) throws RejectedExecutionException;

	/**
	 * Adds the given action to the head of the mailbox. This method will block if the mailbox is full and
	 * should therefore only be called from outside the mailbox main-thread to avoid deadlocks.
	 *
	 * @param priorityAction action to enqueue to the head of the mailbox.
	 * @throws InterruptedException on interruption.
	 * @throws RejectedExecutionException if the mailbox is quiesced or closed.
	 */
	void putAsHead(@Nonnull Runnable priorityAction) throws InterruptedException, RejectedExecutionException;

	/**
	 * Adds the given action to the head of the mailbox if the mailbox is not full. Returns true if the letter
	 * was successfully added to the mailbox.
	 *
	 * @param priorityAction action to enqueue to the head of the mailbox.
	 * @return true if the letter was successfully added.
	 * @throws RejectedExecutionException if the mailbox is quiesced or closed.
	 */
	boolean tryPutAsHead(@Nonnull Runnable priorityAction) throws RejectedExecutionException;

	/**
	 * Handler for letters that are dropped from the mailbox as part of
	 * {@link #clearAndPut(Runnable, DroppedLetterHandler)}.
	 */
	interface DroppedLetterHandler {
		/**
		 * Handle the given dropped letter.
		 * @param droppedLetter the dropped letter.
		 */
		void handle(@Nonnull Runnable droppedLetter);
	}
}
