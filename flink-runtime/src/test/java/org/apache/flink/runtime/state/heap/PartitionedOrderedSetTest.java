//package org.apache.flink.runtime.state.heap;
//
//import org.apache.flink.runtime.state.KeyGroupRange;
//
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Random;
//import java.util.TreeSet;
//
//public class PartitionedOrderedSetTest {
//
//	@Test
//	public void test() {
//		KeyGroupRange keyGroupRange = KeyGroupRange.of(0, 3);
//		PartitionedOrderedSet.SortedFetchingCacheFactory<Integer> factory = new PartitionedOrderedSet.SortedFetchingCacheFactory<Integer>() {
//			@Override
//			public TreeCachingOrderedSetPartition<Integer> createCache(int keyGroup, Comparator<Integer> elementComparator) {
//				return new TreeCachingOrderedSetPartition<Integer>(keyGroup, elementComparator, 64) { //TODO!
//
//					TreeSet<Integer> backend = new TreeSet<>(elementComparator);
//
////					@Override
////					protected void checkConsistency() {
////						final Iterator<Integer> cacheIter = cache.iterator();
////						final Iterator<Integer> backendIter = backend.iterator();
////						while (cacheIter.hasNext()) {
////							Preconditions.checkState(backendIter.next() == cacheIter.next());
////						}
////					}
//
//					@Override
//					protected void addToBackend(Integer element) {
//						backend.add(element);
//					}
//
//					@Override
//					protected void removeFromBackend(Integer element) {
//						backend.remove(element);
//					}
//
//					@Override
//					protected boolean refillCacheFromBackend() {
//						for (Integer elementInOrder : backend) {
//							cache.add(elementInOrder);
//
//							if (cache.size() >= capacity) {
//								break;
//							}
//						}
//						return !backend.isEmpty();
//					}
//
//					@Override
//					protected int size() {
//						return backend.size();
//					}
//
//					@Override
//					public String toString() {
//						return super.toString() + " -> " + backend;
//					}
//				};
//			}
//		};
//
//		PartitionedOrderedSet<Integer> instance = new PartitionedOrderedSet<>(
//			(e) -> e,
//			Integer::compareTo,
//			factory,
//			keyGroupRange,
//			keyGroupRange.getNumberOfKeyGroups());
//
//		Random random = new Random(1);
//
//		int maxTestSize = 256;
//
//		for (int k = 0; k < 100000; ++k) {
//
//			final int testSize = random.nextInt(maxTestSize);
//			HashSet<Integer> check = new HashSet<>(testSize);
//			final int bound = maxTestSize * 100;
//
//			while (check.size() < testSize) {
//				int element = random.nextInt(bound);
//				check.add(element);
//				instance.add(element);
//
//				if (random.nextInt(3) == 0 && !check.isEmpty()) {
//					final Iterator<Integer> iterator = check.iterator();
//					instance.remove(iterator.next());
//					iterator.remove();
//				}
//
//				Assert.assertEquals(check.size(), instance.size());
//			}
//			Assert.assertEquals(check.size(), instance.size());
//			List<Integer> expected = Arrays.asList(check.toArray(new Integer[0]));
//			Collections.sort(expected);
//
//			List<Integer> result = new ArrayList<>(check.size());
//
//			while (!instance.isEmpty()) {
//				final Integer peek = instance.peek();
//				final Integer poll = instance.poll();
//				Assert.assertEquals(poll, peek);
//				result.add(poll);
//				Assert.assertTrue(check.remove(poll));
//				Assert.assertEquals(check.size(), instance.size());
//			}
//
//			Assert.assertNull(instance.peek());
//			Assert.assertNull(instance.poll());
//			Assert.assertEquals(expected, result);
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
