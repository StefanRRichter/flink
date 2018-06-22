//package org.apache.flink.contrib.streaming.state;
//
//import org.apache.flink.api.common.typeutils.base.IntSerializer;
//import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
//import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
//import org.apache.flink.runtime.state.KeyGroupRange;
//import org.apache.flink.runtime.state.heap.CachingOrderedSetPartition;
//import org.apache.flink.runtime.state.heap.PartitionedOrderedSet;
//import org.apache.flink.util.FileUtils;
//
//import org.junit.Test;
//import org.rocksdb.ColumnFamilyDescriptor;
//import org.rocksdb.ColumnFamilyHandle;
//import org.rocksdb.ColumnFamilyOptions;
//import org.rocksdb.DBOptions;
//import org.rocksdb.ReadOptions;
//import org.rocksdb.RocksDB;
//import org.rocksdb.WriteOptions;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.List;
//import java.util.Random;
//
//public class RocksDBOrderedDeleteTest {
//
//	@Test
//	public void test() throws Exception {
//
//		final File file = new File(System.getProperty("java.io.tmpdir"), "rockstest");
//		FileUtils.deleteDirectory(file);
//		final DBOptions dbOptions = PredefinedOptions.FLASH_SSD_OPTIMIZED.createDBOptions();
//		dbOptions.setCreateIfMissing(true);
//		ColumnFamilyOptions columnFamilyOptions = PredefinedOptions.FLASH_SSD_OPTIMIZED.createColumnOptions();
//		List<ColumnFamilyDescriptor> descriptors = Collections.singletonList(new ColumnFamilyDescriptor("default".getBytes(), columnFamilyOptions));
//		List<ColumnFamilyHandle> handles = new ArrayList<>(1);
//		final RocksDB rocksDB = RocksDB.open(dbOptions, file.getAbsolutePath(), descriptors, handles);
//		final ColumnFamilyHandle columnFamily = handles.get(0);
//		WriteOptions writeOptions = new WriteOptions();
//		writeOptions.disableWAL();
//		ReadOptions readOptions = new ReadOptions();
//		try {
//			ByteArrayOutputStreamWithPos outputStreamWithPos = new ByteArrayOutputStreamWithPos(32);
//			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStreamWithPos);
//
//			KeyGroupRange keyGroupRange = KeyGroupRange.of(0, 3);
//			PartitionedOrderedSet.SortedFetchingCacheFactory<Integer> factory = new PartitionedOrderedSet.SortedFetchingCacheFactory<Integer>() {
//				@Override
//				public CachingOrderedSetPartition<Integer> createCache(int keyGroup, Comparator<Integer> elementComparator) {
//					return new RocksDBOrderedSet<>(
//						keyGroup,
//						elementComparator,
//						256,
//						rocksDB,
//						columnFamily,
//						writeOptions,
//						readOptions,
//						IntSerializer.INSTANCE,
//						outputStreamWithPos,
//						outputView,
//						null);
//				}
//			};
//
//			RocksDBOrderedDelete<Integer> instance = new RocksDBOrderedDelete<>(
//				keyGroupRange,
//				keyGroupRange.getNumberOfKeyGroups(),
//				IntSerializer.INSTANCE,
//				rocksDB,
//				columnFamily,
//				writeOptions,
//				readOptions,
//				Integer::compareTo,
//				(e) -> e);
//
//			Random random = new Random(1);
//
//			int maxTestSize = 1000000;
//
//			for (int k = 0; k < 1; ++k) {
//
//				final int testSize = maxTestSize;
////				HashSet<Integer> check = new HashSet<>(testSize);
//				final int bound = maxTestSize * 100;
//
//				int x = 0;
//				while (x++ < testSize) {
//					int element = random.nextInt(bound);
////					check.add(element);
//					instance.add(element);
//
//					if (random.nextInt(3) == 0 /*&& !check.isEmpty()*/) {
////						final Iterator<Integer> iterator = check.iterator();
//						instance.remove(element);
////						iterator.remove();
//					}
//
////					Assert.assertEquals(check.size(), instance.size());
//				}
////				Assert.assertEquals(check.size(), instance.size());
////				List<Integer> expected = Arrays.asList(check.toArray(new Integer[0]));
////				Collections.sort(expected);
//
////				List<Integer> result = new ArrayList<>(check.size());
//
////				while (!instance.isEmpty()) {
////					final Integer peek = instance.peek();
////					final Integer poll = instance.poll();
////					Assert.assertEquals(poll, peek);
//////					result.add(poll);
//////					Assert.assertTrue(check.remove(poll));
//////					if (random.nextInt(3) == 0 && !check.isEmpty()) {
//////						instance.add(iterator.next());
//////						iterator.remove();
//////					}
//////					Assert.assertEquals(check.size(), instance.size());
////				}
////
////				Assert.assertNull(instance.peek());
////				Assert.assertNull(instance.poll());
//
//				instance.poll(Integer.MAX_VALUE);
//
////				Assert.assertEquals(expected, result);
//			}
//		} finally {
//			readOptions.close();
//			writeOptions.close();
//			columnFamily.close();
//			for (ColumnFamilyHandle handle : handles) {
//				handle.close();
//			}
//			rocksDB.close();
//			columnFamilyOptions.close();
//			dbOptions.close();
//			FileUtils.deleteDirectory(file);
//		}
//
////		Collections.sort(check);
////		System.out.println(instance);
////
////		int prev = Integer.MIN_VALUE;
////		int count = 0;
////		while (!instance.isEmpty()) {
////			final int current = poll;
////			System.out.println(current);
////			Assert.assertTrue(current >= prev);
////			prev = current;
////			++count;
////		}
////		Assert.assertEquals(testSize, count);
////		System.out.println(instance);
//	}
//
//}
