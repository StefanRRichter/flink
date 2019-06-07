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

/**
 * A mailbox is basically a blocking queue for inter-thread message exchange in form of {@link Runnable} objects between
 * multiple producer threads and a single consumer.
 */
public interface Mailbox extends MailboxReceiver, MailboxSender {

	/**
	 * The effect of this is that all pending letters in the mailbox are dropped and the given priorityAction
	 * is enqueued to the head of the mailbox. This method should only be invoked by code that has ownership of
	 * the mailbox object and only rarely used, e.g. to submit special events like shutting down the mailbox loop.
	 *
	 * @param priorityAction action to enqueue atomically after the mailbox was cleared.
	 */
	void clearAndPut(@Nonnull Runnable priorityAction);

	/**
	 * Adds the given action to the directly head of the mailbox. The mailbox implementation takes care that
	 * this method never blocks, even if the mailbox is full. This method should only be invoked by code that
	 * has ownership of the mailbox object and only rarely used, e.g. to submit special events like shutting
	 * down the mailbox loop.
	 *
	 * @param priorityAction action to enqueue to the head of the mailbox.
	 */
	void putAsHead(@Nonnull Runnable priorityAction);
}
