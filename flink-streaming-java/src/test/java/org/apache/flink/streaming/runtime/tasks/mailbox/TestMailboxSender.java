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

/**
 * TODO.
 */
public class TestMailboxSender implements Mailbox {

	@Override
	public boolean tryPutMail(@Nonnull Runnable letter) {
		putMail(letter);
		return true;
	}

	@Override
	public void putMail(@Nonnull Runnable letter) {
		letter.run();
	}

	@Override
	public void waitUntilHasCapacity() {
	}

	@Override
	public void open() {

	}

	@Override
	public void quiesce() {

	}

	@Override
	public void close() {

	}

	@Override
	public void clearAndPut(@Nonnull Runnable priorityAction, @Nonnull DroppedLetterHandler droppedLetterHandler) {
		putMail(priorityAction);
	}

	@Override
	public void putAsHead(@Nonnull Runnable priorityAction) {
		putMail(priorityAction);
	}

	@Override
	public boolean tryPutAsHead(@Nonnull Runnable priorityAction) {
		putMail(priorityAction);
		return true;
	}

//	@Override
//	public void quiesce() {
//
//	}

	@Override
	public boolean hasMail() {
		return false;
	}

	@Override
	public Optional<Runnable> tryTakeMail() {
		return Optional.empty();
	}

	@Nonnull
	@Override
	public Runnable takeMail() throws InterruptedException {
		return () -> {};
	}

	@Override
	public void waitUntilHasMail() throws InterruptedException {
	}

//	@Override
//	public void close() throws Exception {
//
//	}
}
