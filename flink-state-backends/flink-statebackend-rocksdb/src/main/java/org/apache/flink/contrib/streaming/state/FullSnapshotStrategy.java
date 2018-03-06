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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.io.async.AbstractAsyncCallableWithResources;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyedBackendStateMetaInfo;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.function.SupplierWithException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.contrib.streaming.state.RocksDBSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.contrib.streaming.state.RocksDBSnapshotUtil.hasMetaDataFollowsFlag;
import static org.apache.flink.contrib.streaming.state.RocksDBSnapshotUtil.setMetaDataFollowsFlagInKey;

public class FullSnapshotStrategy<K> extends SnapshotStrategyBase {

	private static final Logger LOG = LoggerFactory.getLogger(FullSnapshotStrategy.class);

	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> performSnapshot(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory primaryStreamFactory,
		CheckpointOptions checkpointOptions) throws Exception {

		long startTime = System.currentTimeMillis();
		final CloseableRegistry snapshotCloseableRegistry = new CloseableRegistry();

		if (kvStateInformation.isEmpty()) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning null.",
					timestamp);
			}

			return DoneFuture.of(SnapshotResult.empty());
		}

		final SupplierWithException<CheckpointStreamWithResultProvider, Exception> supplier =

			isWithLocalRecovery(
				checkpointOptions.getCheckpointType(),
				localRecoveryConfig.getLocalRecoveryMode()) ?

				() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory,
					localRecoveryConfig.getLocalStateDirectoryProvider()) :

				() -> CheckpointStreamWithResultProvider.createSimpleStream(
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory);

		final RocksDBFullSnapshotOperation<K> snapshotOperation =
			new RocksDBFullSnapshotOperation<>(
				RocksDBKeyedStateBackend.this,
				supplier,
				snapshotCloseableRegistry);

		snapshotOperation.takeDBSnapShot();

		// implementation of the async IO operation, based on FutureTask
		AbstractAsyncCallableWithResources<SnapshotResult<KeyedStateHandle>> ioCallable =
			new AbstractAsyncCallableWithResources<SnapshotResult<KeyedStateHandle>>() {

				@Override
				protected void acquireResources() throws Exception {
					cancelStreamRegistry.registerCloseable(snapshotCloseableRegistry);
					snapshotOperation.openCheckpointStream();
				}

				@Override
				protected void releaseResources() throws Exception {
					closeLocalRegistry();
					releaseSnapshotOperationResources();
				}

				private void releaseSnapshotOperationResources() {
					// hold the db lock while operation on the db to guard us against async db disposal
					snapshotOperation.releaseSnapshotResources();
				}

				@Override
				protected void stopOperation() throws Exception {
					closeLocalRegistry();
				}

				private void closeLocalRegistry() {
					if (cancelStreamRegistry.unregisterCloseable(snapshotCloseableRegistry)) {
						try {
							snapshotCloseableRegistry.close();
						} catch (Exception ex) {
							LOG.warn("Error closing local registry", ex);
						}
					}
				}

				@Nonnull
				@Override
				public SnapshotResult<KeyedStateHandle> performOperation() throws Exception {
					long startTime = System.currentTimeMillis();

					if (isStopped()) {
						throw new IOException("RocksDB closed.");
					}

					snapshotOperation.writeDBSnapshot();

					LOG.info("Asynchronous RocksDB snapshot ({}, asynchronous part) in thread {} took {} ms.",
						primaryStreamFactory, Thread.currentThread(), (System.currentTimeMillis() - startTime));

					return snapshotOperation.getSnapshotResultStateHandle();
				}
			};

		LOG.info("Asynchronous RocksDB snapshot ({}, synchronous part) in thread {} took {} ms.",
			primaryStreamFactory, Thread.currentThread(), (System.currentTimeMillis() - startTime));
		return AsyncStoppableTaskWithCallback.from(ioCallable);
	}
	private boolean isWithLocalRecovery(
		CheckpointType checkpointType,
		LocalRecoveryConfig.LocalRecoveryMode recoveryMode) {
		// we use local recovery when it is activated and we are not taking a savepoint.
		return LocalRecoveryConfig.LocalRecoveryMode.ENABLE_FILE_BASED == recoveryMode
			&& CheckpointType.SAVEPOINT != checkpointType;
	}

	/**
	 * Encapsulates the process to perform a full snapshot of a RocksDBKeyedStateBackend.
	 */
	@VisibleForTesting
	static class RocksDBFullSnapshotOperation<K> {

		private final RocksDB db;
		private final int keyGroupPrefixBytes;
		private final StreamCompressionDecorator keyGroupCompressionDecorator;
		private final TypeSerializer<K> keySerializer;
		private final LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation;

		private final KeyGroupRangeOffsets keyGroupRangeOffsets;
		private final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier;
		private final CloseableRegistry snapshotCloseableRegistry;
		private final ResourceGuard.Lease dbLease;

		private Snapshot snapshot;
		private ReadOptions readOptions;
		private List<Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformationCopy;
		private List<Tuple2<RocksIterator, Integer>> kvStateIterators;

		private CheckpointStreamWithResultProvider checkpointStreamWithResultProvider;
		private DataOutputView outputView;


		public RocksDBFullSnapshotOperation(
			RocksDB db,
			ResourceGuard.Lease dbLease,
			TypeSerializer<K> keySerializer,
			LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation,
			KeyGroupRangeOffsets keyGroupRangeOffsets,
			int keyGroupPrefixBytes,
			CloseableRegistry snapshotCloseableRegistry,
			SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier,
			StreamCompressionDecorator keyGroupCompressionDecorator) {

			this.db = db;
			this.keyGroupPrefixBytes = keyGroupPrefixBytes;
			this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
			this.keySerializer = keySerializer;
			this.kvStateInformation = kvStateInformation;
			this.keyGroupRangeOffsets = keyGroupRangeOffsets;
			this.checkpointStreamSupplier = checkpointStreamSupplier;
			this.snapshotCloseableRegistry = snapshotCloseableRegistry;
			this.dbLease = dbLease;
		}

		@VisibleForTesting
		RocksDBFullSnapshotOperation(
			RocksDBKeyedStateBackend<K> stateBackend,
			SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier,
			CloseableRegistry registry) throws IOException {

			this(
				stateBackend.db,
				stateBackend.rocksDBResourceGuard.acquireResource(),
				stateBackend.keySerializer,
				stateBackend.kvStateInformation,
				new KeyGroupRangeOffsets(stateBackend.keyGroupRange),
				stateBackend.keyGroupPrefixBytes,
				registry,
				checkpointStreamSupplier,
				stateBackend.keyGroupCompressionDecorator);
		}

		/**
		 * 1) Create a snapshot object from RocksDB.
		 *
		 */
		public void takeDBSnapShot() {
			Preconditions.checkArgument(snapshot == null, "Only one ongoing snapshot allowed!");
			this.kvStateInformationCopy = new ArrayList<>(kvStateInformation.values());
			this.snapshot = db.getSnapshot();
		}

		/**
		 * 2) Open CheckpointStateOutputStream through the checkpointStreamFactory into which we will write.
		 *
		 * @throws Exception
		 */
		public void openCheckpointStream() throws Exception {
			Preconditions.checkArgument(checkpointStreamWithResultProvider == null,
				"Output stream for snapshot is already set.");

			checkpointStreamWithResultProvider = checkpointStreamSupplier.get();
			snapshotCloseableRegistry.registerCloseable(checkpointStreamWithResultProvider);
			outputView = new DataOutputViewStreamWrapper(
				checkpointStreamWithResultProvider.getCheckpointOutputStream());
		}

		/**
		 * 3) Write the actual data from RocksDB from the time we took the snapshot object in (1).
		 *
		 * @throws IOException
		 */
		public void writeDBSnapshot() throws IOException, InterruptedException {

			if (null == snapshot) {
				throw new IOException("No snapshot available. Might be released due to cancellation.");
			}

			Preconditions.checkNotNull(checkpointStreamWithResultProvider, "No output stream to write snapshot.");
			writeKVStateMetaData();
			writeKVStateData();
		}

		/**
		 * 4) Returns a snapshot result for the completed snapshot.
		 *
		 * @return snapshot result for the completed snapshot.
		 */
		@Nonnull
		public SnapshotResult<KeyedStateHandle> getSnapshotResultStateHandle() throws IOException {

			if (snapshotCloseableRegistry.unregisterCloseable(checkpointStreamWithResultProvider)) {

				SnapshotResult<StreamStateHandle> res =
					checkpointStreamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
				checkpointStreamWithResultProvider = null;
				return CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult(res, keyGroupRangeOffsets);
			}

			return SnapshotResult.empty();
		}

		/**
		 * 5) Release the snapshot object for RocksDB and clean up.
		 */
		public void releaseSnapshotResources() {

			checkpointStreamWithResultProvider = null;

			if (null != kvStateIterators) {
				for (Tuple2<RocksIterator, Integer> kvStateIterator : kvStateIterators) {
					IOUtils.closeQuietly(kvStateIterator.f0);
				}
				kvStateIterators = null;
			}

			if (null != snapshot) {
				if (null != db) {
					db.releaseSnapshot(snapshot);
				}
				IOUtils.closeQuietly(snapshot);
				snapshot = null;
			}

			if (null != readOptions) {
				IOUtils.closeQuietly(readOptions);
				readOptions = null;
			}

			this.dbLease.close();
		}

		private void writeKVStateMetaData() throws IOException {

			List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> metaInfoSnapshots =
				new ArrayList<>(kvStateInformationCopy.size());

			this.kvStateIterators = new ArrayList<>(kvStateInformationCopy.size());

			int kvStateId = 0;
			for (Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> column :
				kvStateInformationCopy) {

				metaInfoSnapshots.add(column.f1.snapshot());

				//retrieve iterator for this k/v states
				readOptions = new ReadOptions();
				readOptions.setSnapshot(snapshot);

				kvStateIterators.add(
					new Tuple2<>(db.newIterator(column.f0, readOptions), kvStateId));

				++kvStateId;
			}

			KeyedBackendSerializationProxy<K> serializationProxy =
				new KeyedBackendSerializationProxy<>(
					keySerializer,
					metaInfoSnapshots,
					!Objects.equals(
						UncompressedStreamCompressionDecorator.INSTANCE,
						keyGroupCompressionDecorator));

			serializationProxy.write(outputView);
		}

		private void writeKVStateData() throws IOException, InterruptedException {

			byte[] previousKey = null;
			byte[] previousValue = null;
			DataOutputView kgOutView = null;
			OutputStream kgOutStream = null;
			CheckpointStreamFactory.CheckpointStateOutputStream checkpointOutputStream =
				checkpointStreamWithResultProvider.getCheckpointOutputStream();

			try {
				// Here we transfer ownership of RocksIterators to the RocksDBMergeIterator
				try (RocksDBMergeIterator mergeIterator = new RocksDBMergeIterator(
					kvStateIterators, keyGroupPrefixBytes)) {

					// handover complete, null out to prevent double close
					kvStateIterators = null;

					//preamble: setup with first key-group as our lookahead
					if (mergeIterator.isValid()) {
						//begin first key-group by recording the offset
						keyGroupRangeOffsets.setKeyGroupOffset(
							mergeIterator.keyGroup(),
							checkpointOutputStream.getPos());
						//write the k/v-state id as metadata
						kgOutStream = keyGroupCompressionDecorator.decorateWithCompression(checkpointOutputStream);
						kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
						//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
						kgOutView.writeShort(mergeIterator.kvStateId());
						previousKey = mergeIterator.key();
						previousValue = mergeIterator.value();
						mergeIterator.next();
					}

					//main loop: write k/v pairs ordered by (key-group, kv-state), thereby tracking key-group offsets.
					while (mergeIterator.isValid()) {

						assert (!hasMetaDataFollowsFlag(previousKey));

						//set signal in first key byte that meta data will follow in the stream after this k/v pair
						if (mergeIterator.isNewKeyGroup() || mergeIterator.isNewKeyValueState()) {

							//be cooperative and check for interruption from time to time in the hot loop
							checkInterrupted();

							setMetaDataFollowsFlagInKey(previousKey);
						}

						writeKeyValuePair(previousKey, previousValue, kgOutView);

						//write meta data if we have to
						if (mergeIterator.isNewKeyGroup()) {
							//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
							kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
							// this will just close the outer stream
							kgOutStream.close();
							//begin new key-group
							keyGroupRangeOffsets.setKeyGroupOffset(
								mergeIterator.keyGroup(),
								checkpointOutputStream.getPos());
							//write the kev-state
							//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
							kgOutStream = keyGroupCompressionDecorator.decorateWithCompression(checkpointOutputStream);
							kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
							kgOutView.writeShort(mergeIterator.kvStateId());
						} else if (mergeIterator.isNewKeyValueState()) {
							//write the k/v-state
							//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
							kgOutView.writeShort(mergeIterator.kvStateId());
						}

						//request next k/v pair
						previousKey = mergeIterator.key();
						previousValue = mergeIterator.value();
						mergeIterator.next();
					}
				}

				//epilogue: write last key-group
				if (previousKey != null) {
					assert (!hasMetaDataFollowsFlag(previousKey));
					setMetaDataFollowsFlagInKey(previousKey);
					writeKeyValuePair(previousKey, previousValue, kgOutView);
					//TODO this could be aware of keyGroupPrefixBytes and write only one byte if possible
					kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
					// this will just close the outer stream
					kgOutStream.close();
					kgOutStream = null;
				}

			} finally {
				// this will just close the outer stream
				IOUtils.closeQuietly(kgOutStream);
			}
		}

		private void writeKeyValuePair(byte[] key, byte[] value, DataOutputView out) throws IOException {
			BytePrimitiveArraySerializer.INSTANCE.serialize(key, out);
			BytePrimitiveArraySerializer.INSTANCE.serialize(value, out);
		}

		private static void checkInterrupted() throws InterruptedException {
			if (Thread.currentThread().isInterrupted()) {
				throw new InterruptedException("RocksDB snapshot interrupted.");
			}
		}
	}
}

