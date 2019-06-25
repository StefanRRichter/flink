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

/**
 * Interface for the default action that is repeatedly invoked in the mailbox-loop.
 */
public interface MailboxDefaultAction {

	/**
	 * This method implements the default action of the mailbox loop (e.g. processing one event from the input).
	 * Implementations should (in general) be non-blocking.
	 *
	 * @param context context object for collaborative interaction between the default action and the mailbox loop.
	 * @throws Exception on any problems in the action.
	 */
	void performDefaultAction(DefaultActionContext context) throws Exception;

	/**
	 * This context is a feedback interface for the default action to interact with the mailbox execution. In particular
	 * it offers ways to signal that the execution of the default action should be finished or temporarily suspended.
	 */
	interface DefaultActionContext {


		/**
		 * This method must be called to end the stream task when all actions for the tasks have been performed. This
		 * method can be invoked from any thread.
		 */
		void allActionsCompleted();

		/**
		 * Calling this method signals that the mailbox-thread should (temporarily) stop invoking the default action,
		 * e.g. because there is currently no input available. This method must be invoked from the mailbox-thread only!
		 */
		SuspendedDefaultAction suspendDefaultAction();
	}

	/**
	 * Represents the suspended state of the default action, ready to resume.
	 */
	interface SuspendedDefaultAction {

		/**
		 * Resume execution of the default action. Can be called from any thread.
		 */
		void resume();
	}
}
