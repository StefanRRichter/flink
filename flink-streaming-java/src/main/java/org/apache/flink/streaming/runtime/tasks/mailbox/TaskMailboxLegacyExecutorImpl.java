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
 * TODO.
 */
public class TaskMailboxLegacyExecutorImpl implements TaskMailboxExecutor {

	private final Object checkpointLock;
	private final TaskMailboxExecutor delegate;

	public TaskMailboxLegacyExecutorImpl(TaskMailboxExecutor delegate, Object checkpointLock) {
		this.checkpointLock = checkpointLock;
		this.delegate = delegate;
	}

	@Override
	public void execute(@Nonnull Runnable command) {
		assert !isMailboxThread();
		delegate.execute(command);
		synchronized (checkpointLock) {
			checkpointLock.notifyAll();
		}
	}

	@Override
	public boolean tryExecute(Runnable command) {
		boolean success = delegate.tryExecute(command);
		if (success) {
			synchronized (checkpointLock) {
				checkpointLock.notifyAll();
			}
		}
		return success;
	}

	@Override
	public void yield() throws InterruptedException, IllegalStateException {
		assert isMailboxThread();
		if (delegate.isMailboxThread()) {
			while (!delegate.tryYield()) {
				checkpointLock.wait();
			}
		} else {
			checkpointLock.wait();
		}
	}

	@Override
	public boolean tryYield() throws IllegalStateException {
		assert isMailboxThread();
		return delegate.tryYield();
	}

	@Override
	public boolean isMailboxThread() {
		return Thread.holdsLock(checkpointLock);
	}
}
