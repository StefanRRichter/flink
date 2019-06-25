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

import org.apache.flink.core.testutils.OneShotLatch;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link MailboxProcessor}.
 */
public class MailboxProcessorTest {

	@Test
	public void testRunDefaultAction() throws Exception {

		final int expectedInvocations = 3;
		final AtomicInteger counter = new AtomicInteger(0);
		MailboxThread mailboxThread = new MailboxThread() {
			@Override
			public void performDefaultAction(DefaultActionContext context) throws Exception {
				if (counter.incrementAndGet() == expectedInvocations) {
					context.allActionsCompleted();
				}
			}
		};

		start(mailboxThread);
		stop(mailboxThread);
		Assert.assertEquals(expectedInvocations, counter.get());
	}

	@Test
	public void testSignalUnAvailable() throws Exception {

		final AtomicInteger counter = new AtomicInteger(0);
		final AtomicReference<MailboxDefaultAction.SuspendedDefaultAction> suspendedActionRef = new AtomicReference<>();
		final OneShotLatch actionSuspendedLatch = new OneShotLatch();
		final int blockAfterInvocations = 3;
		final int totalInvocations = blockAfterInvocations * 2;

		MailboxThread mailboxThread = new MailboxThread() {
			@Override
			public void performDefaultAction(DefaultActionContext context) {
				if (counter.incrementAndGet() == blockAfterInvocations) {
					suspendedActionRef.set(context.suspendDefaultAction());
					actionSuspendedLatch.trigger();
				} else if (counter.get() == totalInvocations) {
					context.allActionsCompleted();
				}
			}
		};

		MailboxProcessor mailboxProcessor = start(mailboxThread);
		actionSuspendedLatch.await();
		Assert.assertEquals(blockAfterInvocations, counter.get());

		suspendedActionRef.get().resume();
		stop(mailboxThread);
		Assert.assertEquals(totalInvocations, counter.get());
	}

	private static MailboxProcessor start(MailboxThread mailboxThread) {
		mailboxThread.start();
		final MailboxProcessor mailboxProcessor = mailboxThread.getMailboxProcessor();
		mailboxProcessor.openMailbox();
		mailboxThread.signalStart();
		return mailboxProcessor;
	}

	private static void stop(MailboxThread mailboxThread) throws Exception {
		mailboxThread.join();
		MailboxProcessor mailboxProcessor = mailboxThread.getMailboxProcessor();
		mailboxProcessor.prepareShutDown();
		mailboxProcessor.shutDown();
		mailboxThread.checkException();
	}

	static class MailboxThread extends Thread implements MailboxDefaultAction {

		MailboxProcessor mailboxProcessor;
		OneShotLatch mailboxCreatedLatch = new OneShotLatch();
		OneShotLatch canRun = new OneShotLatch();
		private Throwable caughtException;

		@Override
		public final void run() {
			mailboxProcessor = new MailboxProcessor(this);
			mailboxCreatedLatch.trigger();
			try {
				canRun.await();
				mailboxProcessor.runMailboxLoop();
			} catch (Throwable t) {
				this.caughtException = t;
			}
		}

		@Override
		public void performDefaultAction(DefaultActionContext context) throws Exception {
			context.allActionsCompleted();
		}

		final MailboxProcessor getMailboxProcessor() {
			try {
				mailboxCreatedLatch.await();
				return mailboxProcessor;
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		}

		final void signalStart() {
			if (mailboxCreatedLatch.isTriggered()) {
				canRun.trigger();
			}
		}

		void checkException() throws Exception {
			if (caughtException != null) {
				throw new Exception(caughtException);
			}
		}
	}

}
