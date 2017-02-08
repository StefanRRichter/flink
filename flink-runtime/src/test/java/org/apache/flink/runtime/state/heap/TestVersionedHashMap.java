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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TestVersionedHashMap {

	@Test
	public void testCopyOnWrite() {

		RegisteredBackendStateMetaInfo<Integer, ArrayList<Integer>> metaInfo =
				new RegisteredBackendStateMetaInfo<>(
						StateDescriptor.Type.UNKNOWN,
						"test",
						IntSerializer.INSTANCE,
						new ArrayListSerializer<>(IntSerializer.INSTANCE));

		StateTable<Integer, Integer, ArrayList<Integer>> map =
				new StateTable<>(16, metaInfo , new KeyGroupRange(0, 0), 1);

		HashMap<Tuple2<Integer, Integer>, ArrayList<Integer>> referenceMap = new HashMap<>();

		Random rand = new Random(42);

		Tuple3<Integer, Integer, ArrayList<Integer>>[] snapshot = null;
		Tuple3<Integer, Integer, ArrayList<Integer>>[] reference = null;
		Tuple3<Integer, Integer, ArrayList<Integer>>[] prevSnapshot = null;
		Tuple3<Integer, Integer, ArrayList<Integer>>[] prevReference = null;

		for (int i = 0; i < 100_000; ++i) {
			int key = rand.nextInt(1000);
			int namespace = rand.nextInt(10);
			int val = rand.nextInt();

			ArrayList<Integer> list = map.get(key, namespace);
			Tuple2<Integer, Integer> compositeKey = new Tuple2<>(key, namespace);
			ArrayList<Integer> referenceList = referenceMap.get(compositeKey);
			if (null == list) {
				Assert.assertNull(referenceList);
				list = new ArrayList<>();
				referenceList = new ArrayList<>();
				map.put(key, namespace, list);
				referenceMap.put(compositeKey, referenceList);
			}
			list.add(val);
			referenceList.add(val);

			if (i % 5_000 == 0) {

				Assert.assertTrue(deepCompare(snapshot, reference));
				Assert.assertTrue(deepCompare(prevSnapshot, prevReference));

				prevSnapshot = snapshot;
				prevReference = reference;

				snapshot = map.snapshotDump();
				reference = manualDeepDump(referenceMap);
			}
		}
	}

//	@Test
//	public void performanceTest() {
//		VersionedHashMap<Integer, Integer, ArrayList<Integer>> map =
//				new VersionedHashMap<>(16, new ArrayListSerializer<>(IntSerializer.INSTANCE));
//
//		Random rand = new Random(42);
//
//		Tuple3<Integer, Integer, ArrayList<Integer>>[] snapshot = null;
//		long t = System.nanoTime();
//		for (int i = 0; i < 10_000_000; ++i) {
//			int key = rand.nextInt(10000);
//			int namespace = rand.nextInt(100);
//			int val = rand.nextInt();
//
//			ArrayList<Integer> list = map.get(key, namespace);
//			if (null == list) {
//				list = new ArrayList<>();
//				map.put(key, namespace, list);
//			}
//			list.add(val);
//
//			if (i % 10_000 == 0) {
//				snapshot = map.versionCopy();
//				if(snapshot.length < 0) {
//					System.out.println("!");
//				}
//			}
//		}
//		System.out.println("time: "+(System.nanoTime() -t));
//	}
//
//	@Test
//	public void performanceTest2() {
//		HashMap<Tuple2<Integer, Integer>, ArrayList<Integer>> referenceMap = new HashMap<>();
//
//		Random rand = new Random(42);
//
//		Tuple3<Integer, Integer, ArrayList<Integer>>[] reference = null;
//
//		long t = System.nanoTime();
//
//		for (int i = 0; i < 10_000_000; ++i) {
//			int key = rand.nextInt(10000);
//			int namespace = rand.nextInt(100);
//			int val = rand.nextInt();
//
//			Tuple2<Integer, Integer> compositeKey = new Tuple2<>(key, namespace);
//			ArrayList<Integer> referenceList = referenceMap.get(compositeKey);
//			if (null == referenceList) {
//				referenceList = new ArrayList<>();
//				referenceMap.put(compositeKey, referenceList);
//			}
//			referenceList.add(val);
//
//			if (i % 10_000 == 0) {
//				reference = manualDeepDump(referenceMap);
//				if(reference.length < 0) {
//					System.out.println("!");
//				}
//			}
//		}
//		System.out.println("time: "+(System.nanoTime() -t));
//	}

	public Tuple3<Integer, Integer, ArrayList<Integer>>[] manualDeepDump(
			HashMap<Tuple2<Integer, Integer>, ArrayList<Integer>> map) {

		Tuple3<Integer, Integer, ArrayList<Integer>>[] result = new Tuple3[map.size()];
		int pos = 0;
		for (Map.Entry<Tuple2<Integer, Integer>, ArrayList<Integer>> entry : map.entrySet()) {
			Integer key = entry.getKey().f0;
			Integer namespace = entry.getKey().f1;
			result[pos++] = new Tuple3<>(key, namespace, new ArrayList<>(entry.getValue()));
		}
		return result;
	}

	public boolean deepCompare(Tuple3<Integer, Integer, ArrayList<Integer>>[] a, Tuple3<Integer, Integer, ArrayList<Integer>>[] b) {

		if (a == b) {
			return true;
		}

		if (a.length != b.length) {
			return false;
		}

		Comparator<Tuple3<Integer, Integer, ArrayList<Integer>>> comparator =
				new Comparator<Tuple3<Integer, Integer, ArrayList<Integer>>>() {

					@Override
					public int compare(Tuple3<Integer, Integer, ArrayList<Integer>> o1, Tuple3<Integer, Integer, ArrayList<Integer>> o2) {
						int namespaceDiff = o1.f1 - o2.f1;
						return namespaceDiff != 0 ? namespaceDiff : o1.f0 - o2.f0;
					}
				};

		Arrays.sort(a, comparator);
		Arrays.sort(b, comparator);

		for (int i = 0; i < a.length; ++i) {
			Tuple3<Integer, Integer, ArrayList<Integer>> av = a[i];
			Tuple3<Integer, Integer, ArrayList<Integer>> bv = b[i];
			if (!av.f0.equals(bv.f0)) {
				return false;
			}

			if (!av.f1.equals(bv.f1)) {
				return false;
			}

			Collections.sort(av.f2);
			Collections.sort(bv.f2);

			if (!av.f2.equals(bv.f2)) {
				return false;
			}
		}

		return true;
	}

}