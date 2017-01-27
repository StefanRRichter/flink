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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.AbstractCloseableRegistry;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingProxyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This implementation of an {@link AbstractCloseableRegistry} registers {@link WrappingProxyCloseable}. When
 * the proxy becomes subject to GC, this registry takes care of closing unclosed {@link Closeable}s.
 * <p>
 * Phantom references are used to track when {@link org.apache.flink.util.WrappingProxy}s of {@link Closeable} got
 * GC'ed. We ensure that the wrapped {@link Closeable} is properly closed to avoid resource leaks.
 * <p>
 * Other than that, it works like a normal {@link CloseableRegistry}.
 * <p>
 * All methods in this class are thread-safe.
 */
@Internal
public class SafetyNetCloseableRegistry extends
		AbstractCloseableRegistry<WrappingProxyCloseable<? extends Closeable>,
				SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef> {

	/** Singleton reaper thread takes care of all registries in VM */
	private static final CloseableReaperThread REAPER_THREAD = new CloseableReaperThread();
	private static final Logger LOG = LoggerFactory.getLogger(SafetyNetCloseableRegistry.class);

	static {
		REAPER_THREAD.start();
	}

	public SafetyNetCloseableRegistry() {
		super(new IdentityHashMap<Closeable, PhantomDelegatingCloseableRef>());
	}

	@Override
	protected void doRegister(
			WrappingProxyCloseable<? extends Closeable> wrappingProxyCloseable,
			Map<Closeable, PhantomDelegatingCloseableRef> closeableMap) throws IOException {

		Closeable innerCloseable = WrappingProxyUtil.stripProxy(wrappingProxyCloseable.getWrappedDelegate());

		if (null == innerCloseable) {
			return;
		}

		PhantomDelegatingCloseableRef phantomRef = new PhantomDelegatingCloseableRef(
				wrappingProxyCloseable,
				this,
				REAPER_THREAD.referenceQueue);

		closeableMap.put(innerCloseable, phantomRef);
	}

	@Override
	protected void doUnRegister(
			WrappingProxyCloseable<? extends Closeable> closeable,
			Map<Closeable, PhantomDelegatingCloseableRef> closeableMap) {

		Closeable innerCloseable = WrappingProxyUtil.stripProxy(closeable.getWrappedDelegate());

		if (null == innerCloseable) {
			return;
		}

		closeableMap.remove(innerCloseable);
	}

	/**
	 * Phantom reference to {@link WrappingProxyCloseable}.
	 */
	static final class PhantomDelegatingCloseableRef
			extends PhantomReference<WrappingProxyCloseable<? extends Closeable>>
			implements Closeable {

		private final Closeable innerCloseable;
		private final SafetyNetCloseableRegistry closeableRegistry;
		private final String debugString;

		public PhantomDelegatingCloseableRef(
				WrappingProxyCloseable<? extends Closeable> referent,
				SafetyNetCloseableRegistry closeableRegistry,
				ReferenceQueue<? super WrappingProxyCloseable<? extends Closeable>> q) {

			super(referent, q);
			this.innerCloseable = Preconditions.checkNotNull(WrappingProxyUtil.stripProxy(referent));
			this.closeableRegistry = Preconditions.checkNotNull(closeableRegistry);
			this.debugString = referent.toString();
		}

		public String getDebugString() {
			return debugString;
		}

		@Override
		public void close() throws IOException {
			synchronized (closeableRegistry.getSynchronizationLock()) {
				closeableRegistry.closeableToRef.remove(innerCloseable);
			}
			innerCloseable.close();
		}
	}

	/**
	 * Reaper runnable collects and closes leaking resources
	 */
	static final class CloseableReaperThread extends Thread {

		private CloseableReaperThread() {
			super("CloseableReaperThread");
			this.referenceQueue = new ReferenceQueue<>();
			this.running = false;
			this.setDaemon(true);
		}

		private volatile boolean running;
		private final ReferenceQueue<WrappingProxyCloseable<? extends Closeable>> referenceQueue;

		@Override
		public void run() {
			this.running = true;
			try {
				List<PhantomDelegatingCloseableRef> closeableList = new LinkedList<>();
				while (running) {
					PhantomDelegatingCloseableRef oldRef = (PhantomDelegatingCloseableRef) referenceQueue.remove();

					do {
						closeableList.add(oldRef);
					}
					while ((oldRef = (PhantomDelegatingCloseableRef) referenceQueue.poll()) != null);

					// close outside the synchronized block in case this is blocking
					for (PhantomDelegatingCloseableRef closeableRef : closeableList) {
						IOUtils.closeQuietly(closeableRef);
						if (LOG.isDebugEnabled()) {
							LOG.debug("Closing unclosed resource: " + closeableRef.getDebugString());
						}
					}

					closeableList.clear();
				}
			} catch (InterruptedException e) {
				// done
			}
		}

		@Override
		public void interrupt() {
			this.running = false;
			super.interrupt();
		}
	}

	public static void stopReaperThread() {
		REAPER_THREAD.interrupt();
	}
}
