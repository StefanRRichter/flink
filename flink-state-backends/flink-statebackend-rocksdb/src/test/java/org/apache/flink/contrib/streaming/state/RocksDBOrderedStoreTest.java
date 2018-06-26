package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.CachingInternalPriorityQueue;
import org.apache.flink.runtime.state.heap.PartitionedOrderedSet;
import org.apache.flink.runtime.state.heap.TreeOrderedCache;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class RocksDBOrderedStoreTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private void runTestWithRocksInstance(
		ThrowingConsumer<CachingInternalPriorityQueue.OrderedStore<Integer>, Exception> testMethod) throws Exception {

		final File rocksFolder = temporaryFolder.newFolder();
		final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);

		try (final DBOptions dbOptions = PredefinedOptions.FLASH_SSD_OPTIMIZED.createDBOptions().setCreateIfMissing(true);
			 final ColumnFamilyOptions columnFamilyOptions = PredefinedOptions.FLASH_SSD_OPTIMIZED.createColumnOptions();
			 final WriteOptions writeOptions = new WriteOptions();
			 final ReadOptions readOptions = new ReadOptions();
			 final RocksDB rocksDB = RocksDB.open(
				 dbOptions,
				 rocksFolder.getAbsolutePath(),
				 Collections.singletonList(new ColumnFamilyDescriptor("default".getBytes(), columnFamilyOptions)),
				 columnFamilyHandles);
			 final ColumnFamilyHandle defaultColumnFamily = columnFamilyHandles.get(0);
			 final RocksDBWriteBatchWrapper batchWrapper = new RocksDBWriteBatchWrapper(rocksDB, writeOptions);
			 final ByteArrayOutputStreamWithPos outputStreamWithPos = new ByteArrayOutputStreamWithPos(32);
			 final DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStreamWithPos)) {

			writeOptions.disableWAL();

			final CachingInternalPriorityQueue.OrderedStore<Integer> store = new RocksDBOrderedStore<>(
				0,
				rocksDB,
				defaultColumnFamily,
				readOptions,
				IntSerializer.INSTANCE,
				outputStreamWithPos,
				outputView,
				batchWrapper);

			testMethod.accept(store);
		}
	}

	private void testAddImpl(CachingInternalPriorityQueue.OrderedStore<Integer> store) {
		Assert.assertEquals(0, store.size());
		store.remove(41);
		Assert.assertEquals(0, store.size());
		store.add(41);
		Assert.assertEquals(1, store.size());
		store.add(42);
		Assert.assertEquals(2, store.size());
		store.add(43);
		Assert.assertEquals(3, store.size());
		store.add(44);
		Assert.assertEquals(4, store.size());
		store.add(45);
		Assert.assertEquals(4, store.size());

	}

	@Test
	public void testAdd() throws Exception {
		runTestWithRocksInstance(this::testAddImpl);
	}

//	void remove(E element);
//
//	boolean refillCacheFromBackend(OrderedCache<E> cacheToFill);

	@Test
	public void test() throws Exception {

		final File file = new File(System.getProperty("java.io.tmpdir"), "rockstest");
		FileUtils.deleteDirectory(file);
		final DBOptions dbOptions = PredefinedOptions.FLASH_SSD_OPTIMIZED.createDBOptions();
		dbOptions.setCreateIfMissing(true);
		ColumnFamilyOptions columnFamilyOptions = PredefinedOptions.FLASH_SSD_OPTIMIZED.createColumnOptions();
		List<ColumnFamilyDescriptor> descriptors = Collections.singletonList(new ColumnFamilyDescriptor("default".getBytes(), columnFamilyOptions));
		List<ColumnFamilyHandle> handles = new ArrayList<>(1);
		final RocksDB rocksDB = RocksDB.open(dbOptions, file.getAbsolutePath(), descriptors, handles);
		final ColumnFamilyHandle columnFamily = handles.get(0);
		WriteOptions writeOptions = new WriteOptions();

		ReadOptions readOptions = new ReadOptions();
		RocksDBWriteBatchWrapper batchWrapper = new RocksDBWriteBatchWrapper(rocksDB, writeOptions);
		KeyGroupRange keyGroupRange = KeyGroupRange.of(0, 3);
		int cacheCapacity = 32*4*1024 / keyGroupRange.getNumberOfKeyGroups();
		try {
			ByteArrayOutputStreamWithPos outputStreamWithPos = new ByteArrayOutputStreamWithPos(32);
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStreamWithPos);


			PartitionedOrderedSet.SortedFetchingCacheFactory<Integer> factory = new PartitionedOrderedSet.SortedFetchingCacheFactory<Integer>() {
				@Override
				public CachingInternalPriorityQueue<Integer> createCache(int keyGroup, Comparator<Integer> elementComparator) {

					final CachingInternalPriorityQueue.OrderedCache<Integer> cache = new TreeOrderedCache<>(elementComparator, cacheCapacity);
					final CachingInternalPriorityQueue.OrderedStore<Integer> store = new RocksDBOrderedStore<>(
						keyGroup,
						rocksDB,
						columnFamily,
						readOptions,
						IntSerializer.INSTANCE,
						outputStreamWithPos,
						outputView,
						batchWrapper);
					return new CachingInternalPriorityQueue<>(cache, store);
				}
			};

			PartitionedOrderedSet<Integer> instance = new PartitionedOrderedSet<>(
				(e) -> e,
				Integer::compareTo,
				factory,
				keyGroupRange,
				keyGroupRange.getNumberOfKeyGroups());

			Random random = new Random(1);

			int maxTestSize = 1000000;

			for (int k = 0; k < 1; ++k) {

				final int testSize = maxTestSize;
//				HashSet<Integer> check = new HashSet<>(testSize);
				final int bound = maxTestSize * 100;

				long t = System.currentTimeMillis();

				int x = 0;
				while (x++ < testSize) {
					int element = random.nextInt(bound);
					//check.add(element);
					instance.add(element);

					if (random.nextInt(3) == 0 /*&& !check.isEmpty()*/) {
						//final Iterator<Integer> iterator = check.iterator();
						instance.remove(element);
						//iterator.remove();
					}

//					instance.add(element);
//					while(!instance.isEmpty()) {
//						instance.poll();
//					}

					//Assert.assertEquals(check.size(), instance.size());
				}
				long nt = System.currentTimeMillis();
				System.out.println("insert "+(nt - t));
				//Assert.assertEquals(check.size(), instance.size());
//				List<Integer> expected = Arrays.asList(check.toArray(new Integer[0]));
//				Collections.sort(expected);
//
//				List<Integer> result = new ArrayList<>(check.size());

				t=System.currentTimeMillis();
				while (!instance.isEmpty()) {
					final Integer peek = instance.peek();
					final Integer poll = instance.poll();
					Assert.assertEquals(poll, peek);
//					result.add(poll);
//					Assert.assertTrue(check.remove(poll));
					//Assert.assertEquals(check.size(), instance.size());
				}

				nt = System.currentTimeMillis();
				System.out.println("iter "+(nt - t));

				Assert.assertNull(instance.peek());
				Assert.assertNull(instance.poll());
//				Assert.assertEquals(expected, result);
			}
		} finally {
			batchWrapper.close();
			readOptions.close();
			writeOptions.close();
			columnFamily.close();
			for (ColumnFamilyHandle handle : handles) {
				handle.close();
			}
			rocksDB.close();
			columnFamilyOptions.close();
			dbOptions.close();
			FileUtils.deleteDirectory(file);
		}

//		Collections.sort(check);
//		System.out.println(instance);
//
//		int prev = Integer.MIN_VALUE;
//		int count = 0;
//		while (!instance.isEmpty()) {
//			final int current = poll;
//			System.out.println(current);
//			Assert.assertTrue(current >= prev);
//			prev = current;
//			++count;
//		}
//		Assert.assertEquals(testSize, count);
//		System.out.println(instance);
	}

}
