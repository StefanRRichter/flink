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

import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link MailboxImpl}.
 */
public class MailboxImplTest {

	private static final Runnable POISON_LETTER = () -> {};
	private static final int CAPACITY_POW_2 = 2;
	private static final int CAPACITY = 1 << CAPACITY_POW_2;

	/**
	 * Object under test.
	 */
	private Mailbox mailbox;

	@Before
	public void setUp() throws Exception {
		mailbox = new MailboxImpl(CAPACITY_POW_2);
	}

	/**
	 * Test for #clearAndPut should remove other pending events and enqueue directly to the head of the mailbox queue.
	 */
	@Test
	public void testClearAndPut() {
		for (int i = 0; i < CAPACITY; ++i) {
			Assert.assertTrue(mailbox.tryPutMail(() -> {}));
		}

		mailbox.clearAndPut(POISON_LETTER);

		Assert.assertTrue(mailbox.hasMail());
		Assert.assertEquals(POISON_LETTER, mailbox.tryTakeMail().get());
		Assert.assertFalse(mailbox.hasMail());
	}

	/**
	 * Test that #putAsHead works if the queue is not full.
	 */
	@Test
	public void testPutAsHeadIfMoreCapacity() {

		final AtomicInteger validator = new AtomicInteger(0);

		mailbox.putAsHead(validator::incrementAndGet);
		mailbox.tryTakeMail().ifPresent(Runnable::run);
		Assert.assertFalse(mailbox.tryTakeMail().isPresent());
		Assert.assertEquals(1, validator.get());

		Assert.assertTrue(mailbox.tryPutMail(() -> validator.compareAndSet(2, 3)));
		mailbox.putAsHead(() -> validator.compareAndSet(1, 2));

		mailbox.tryTakeMail().ifPresent(Runnable::run);
		mailbox.tryTakeMail().ifPresent(Runnable::run);

		Assert.assertEquals(3, validator.get());
	}

	/**
	 * Test that #putAsHead works if the head is full and even multiple times in a row.
	 */
	@Test
	public void testPutAsHeadIfFull() {
		testPutAsHeadIfFullInternal(1);
		testPutAsHeadIfFullInternal(2);
		testPutAsHeadIfFullInternal(CAPACITY);
		testPutAsHeadIfFullInternal(CAPACITY + 1);
	}

	private void testPutAsHeadIfFullInternal(int numOverflowPuts) {

		final AtomicInteger validator = new AtomicInteger(0);

		for (int i = validator.get(); i < CAPACITY; ++i) {
			final int val = numOverflowPuts + i;
			Assert.assertTrue(mailbox.tryPutMail(() -> validator.compareAndSet(val, val + 1)));
		}

		Assert.assertFalse(mailbox.tryPutMail(() -> {}));

		for (int i = 0; i < numOverflowPuts; ++i) {
			final int val = numOverflowPuts - i - 1;
			mailbox.putAsHead(() -> validator.compareAndSet(val, val + 1));
		}

		Optional<Runnable> runnable;
		while ((runnable = mailbox.tryTakeMail()).isPresent()) {
			runnable.get().run();
		}

		Assert.assertEquals(CAPACITY + numOverflowPuts, validator.get());
	}

	@Test
	public void testContracts() throws Exception {
		final Queue<Runnable> testObjects = new LinkedList<>();
		Assert.assertFalse(mailbox.hasMail());

		for (int i = 0; i < CAPACITY; ++i) {
			Runnable letter = () -> {};
			testObjects.add(letter);
			Assert.assertTrue(mailbox.tryPutMail(letter));
			Assert.assertTrue(mailbox.hasMail());
		}

		Assert.assertFalse(mailbox.tryPutMail(() -> {}));

		while (!testObjects.isEmpty()) {
			Assert.assertEquals(testObjects.remove(), mailbox.tryTakeMail().get());
			Assert.assertEquals(!testObjects.isEmpty(), mailbox.hasMail());
			mailbox.waitUntilHasCapacity(); // should not block here because the mailbox is not full
		}

		Thread waitingReader = new Thread(ThrowingRunnable.unchecked(() -> mailbox.waitUntilHasMail()));
		waitingReader.start();
		Thread.sleep(1);
		Assert.assertTrue(waitingReader.isAlive());
		mailbox.tryPutMail(() -> {});
		waitingReader.join(); // should complete here

		while (mailbox.tryPutMail(() -> {})) {}

		Thread waitingWriter = new Thread(ThrowingRunnable.unchecked(() -> mailbox.waitUntilHasCapacity()));
		waitingWriter.start();
		Thread.sleep(1);
		Assert.assertTrue(waitingWriter.isAlive());
		mailbox.takeMail();
		waitingWriter.join();
	}

	/**
	 * Test the producer-consumer pattern using the blocking methods on the mailbox.
	 */
	@Test
	public void testConcurrentPutTakeBlocking() throws Exception {
		testPutTake(MailboxReceiver::takeMail, MailboxSender::putMail);
	}

	/**
	 * Test the producer-consumer pattern using the non-blocking methods & waits on the mailbox.
	 */
	@Test
	public void testConcurrentPutTakeNonBlockingAndWait() throws Exception {
		testPutTake((mailbox -> {
				mailbox.waitUntilHasMail();
				return mailbox.tryTakeMail().get();
			}),
			((mailbox, runnable) -> {
				while (!mailbox.tryPutMail(runnable)) {
					mailbox.waitUntilHasCapacity();
				}
			}));
	}

	/**
	 * Test producer-consumer pattern through the mailbox in a concurrent setting (n-writer / 1-reader).
	 */
	private void testPutTake(
		FunctionWithException<Mailbox, Runnable, Exception> takeMethod,
		BiConsumerWithException<Mailbox, Runnable, Exception> putMethod) throws Exception {
		final int numThreads = 10;
		final int numLettersPerThread = 1000;
		final int[] results = new int[numThreads];
		Thread[] writerThreads = new Thread[numThreads];
		Thread readerThread = new Thread(ThrowingRunnable.unchecked(() -> {
			Runnable letter;
			while ((letter = takeMethod.apply(mailbox)) != POISON_LETTER) {
				letter.run();
			}
		}));

		readerThread.start();
		for (int i = 0; i < writerThreads.length; ++i) {
			final int threadId = i;
			writerThreads[i] = new Thread(ThrowingRunnable.unchecked(() -> {
				for (int k = 0; k < numLettersPerThread; ++k) {
					putMethod.accept(mailbox, () -> ++results[threadId]);
				}
			}));
		}

		for (Thread writerThread : writerThreads) {
			writerThread.start();
		}

		for (Thread writerThread : writerThreads) {
			writerThread.join();
		}

		mailbox.putMail(POISON_LETTER);

		readerThread.join();
		for (int perThreadResult : results) {
			Assert.assertEquals(numLettersPerThread, perThreadResult);
		}
	}
}
