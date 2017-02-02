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

public class StateTableTest {

	@Test
	public void testPutGetRemoveContains() {
		RegisteredBackendStateMetaInfo<Integer, ArrayList<Integer>> metaInfo =
				new RegisteredBackendStateMetaInfo<>(
						StateDescriptor.Type.UNKNOWN,
						"test",
						IntSerializer.INSTANCE,
						new ArrayListSerializer<>(IntSerializer.INSTANCE)); // we use mutable state objects.

		StateTable<Integer, Integer, ArrayList<Integer>> stateTable =
				new StateTable<>(8, metaInfo);

		ArrayList<Integer> state_1_1 = new ArrayList<>();
		state_1_1.add(41);
		ArrayList<Integer> state_2_1 = new ArrayList<>();
		state_2_1.add(42);
		ArrayList<Integer> state_1_2 = new ArrayList<>();
		state_1_2.add(43);

		Assert.assertNull(stateTable.putAndGetOld(1, 1, state_1_1));
		Assert.assertEquals(state_1_1, stateTable.get(1, 1));
		Assert.assertEquals(1, stateTable.size());

		Assert.assertNull(stateTable.putAndGetOld(2, 1, state_2_1));
		Assert.assertEquals(state_2_1, stateTable.get(2, 1));
		Assert.assertEquals(2, stateTable.size());

		Assert.assertNull(stateTable.putAndGetOld(1, 2, state_1_2));
		Assert.assertEquals(state_1_2, stateTable.get(1, 2));
		Assert.assertEquals(3, stateTable.size());

		Assert.assertTrue(stateTable.containsKey(2, 1));
		Assert.assertFalse(stateTable.containsKey(3, 1));
		Assert.assertFalse(stateTable.containsKey(2, 3));
		stateTable.put(2, 1, null);
		Assert.assertTrue(stateTable.containsKey(2, 1));
		Assert.assertEquals(3, stateTable.size());
		Assert.assertNull(stateTable.get(2, 1));
		stateTable.put(2, 1, state_2_1);
		Assert.assertEquals(3, stateTable.size());

		Assert.assertEquals(state_2_1, stateTable.removeAndGetOld(2, 1));
		Assert.assertFalse(stateTable.containsKey(2, 1));
		Assert.assertEquals(2, stateTable.size());

		stateTable.remove(1, 2);
		Assert.assertFalse(stateTable.containsKey(1, 2));
		Assert.assertEquals(1, stateTable.size());

		Assert.assertNull(stateTable.removeAndGetOld(4, 2));
		Assert.assertEquals(1, stateTable.size());
	}

	@Test
	public void testIncrementalRehash() {
		RegisteredBackendStateMetaInfo<Integer, ArrayList<Integer>> metaInfo =
				new RegisteredBackendStateMetaInfo<>(
						StateDescriptor.Type.UNKNOWN,
						"test",
						IntSerializer.INSTANCE,
						new ArrayListSerializer<>(IntSerializer.INSTANCE)); // we use mutable state objects.

		StateTable<Integer, Integer, ArrayList<Integer>> stateTable =
				new StateTable<>(128, metaInfo);

		int insert = 0;
		int remove = 0;
		while (!stateTable.isRehashing()) {
			stateTable.put(insert++, 0, new ArrayList<Integer>());
			if(insert % 8 == 0) {
				stateTable.remove(remove++, 0);
			}
		}
		Assert.assertEquals(insert - remove, stateTable.size());
		while (stateTable.isRehashing()) {
			stateTable.put(insert++, 0, new ArrayList<Integer>());
			if(insert % 8 == 0) {
				stateTable.remove(remove++, 0);
			}
		}
		Assert.assertEquals(insert - remove, stateTable.size());

		for (int i = 0; i < insert; ++i) {
			if (i < remove) {
				Assert.assertFalse(stateTable.containsKey(i, 0));
			} else {
				Assert.assertTrue(stateTable.containsKey(i, 0));
			}
		}
	}

	/**
	 * This test does some random modifications to a state table and a reference (hash map). Then draws snapshots,
	 * does more modifications and checks snapshot integrity.
	 */
	@Test
	public void testRandomModificationsAndCopyOnWrite() {

		RegisteredBackendStateMetaInfo<Integer, ArrayList<Integer>> metaInfo =
				new RegisteredBackendStateMetaInfo<>(
						StateDescriptor.Type.UNKNOWN,
						"test",
						IntSerializer.INSTANCE,
						new ArrayListSerializer<>(IntSerializer.INSTANCE)); // we use mutable state objects.

		StateTable<Integer, Integer, ArrayList<Integer>> stateTable =
				new StateTable<>(128, metaInfo);

		HashMap<Tuple2<Integer, Integer>, ArrayList<Integer>> referenceMap = new HashMap<>();

		Random rand = new Random(42);

		StateTable.StateTableEntry<Integer, Integer, ArrayList<Integer>>[] snapshot = null;
		int snapshotSize = 0;
		Tuple3<Integer, Integer, ArrayList<Integer>>[] reference = null;

		int val = 0;
		int snapshotId = 0;

		// the main loop for modifications
		for (int i = 0; i < 500_000; ++i) {

			int key = rand.nextInt(1000);
			int namespace = rand.nextInt(10);
			Tuple2<Integer, Integer> compositeKey = new Tuple2<>(key, namespace);

			int op = rand.nextInt(6);

			ArrayList<Integer> state = null;
			ArrayList<Integer> referenceState = null;
			switch (op) {
				case 0:
				case 1: {
					state = stateTable.get(key, namespace);
					referenceState = referenceMap.get(compositeKey);
					if (null == state) {
						if (null != referenceState) {
							throw new IllegalStateException();
						}
						state = new ArrayList<>();
						stateTable.put(key, namespace, state);
						referenceState = new ArrayList<>();
						referenceMap.put(compositeKey, referenceState);
					}
					break;
				}
				case 2: {
					stateTable.put(key, namespace, new ArrayList<Integer>());
					referenceMap.put(compositeKey, new ArrayList<Integer>());
					break;
				}
				case 3: {
					state = stateTable.putAndGetOld(key, namespace, new ArrayList<Integer>());
					referenceState = referenceMap.put(compositeKey, new ArrayList<Integer>());
					break;
				}
				case 4: {
					stateTable.remove(key, namespace);
					referenceMap.remove(compositeKey);
					break;
				}
				case 5: {
					state = stateTable.removeAndGetOld(key, namespace);
					referenceState = referenceMap.remove(compositeKey);
					break;
				}
				default: {
					throw new IllegalStateException();
				}
			}

			Assert.assertEquals(referenceMap.size(), stateTable.size());

			if (state != null) {
				Assert.assertEquals(referenceState.size(), state.size());
				if (rand.nextBoolean() && !state.isEmpty()) {
					state.remove(state.size() - 1);
					referenceState.remove(referenceState.size() - 1);
				} else {
					state.add(val);
					referenceState.add(val);
					++val;
				}
			}

			// snapshot triggering / comparison / release
			if (i > 0 && i % 500 == 0) {

				if (snapshot != null) {
					Assert.assertTrue(deepCompare(convert(snapshot, snapshotSize), reference));
					if(i % 5_000 == 0) {
						snapshot = null;
						reference = null;
						snapshotSize = 0;
						stateTable.releaseSnapshot(snapshotId);
					}
				} else {
					++snapshotId;
					snapshot = stateTable.snapshotTableArrays();
					snapshotSize = stateTable.size();
					reference = manualDeepDump(referenceMap);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static <K, N, S> Tuple3<K, N, S>[] convert(StateTable.StateTableEntry<K, N, S>[] snapshot, int mapSize) {
		Tuple3<K, N, S>[] result = new Tuple3[mapSize];
		int pos = 0;
		for (StateTable.StateTableEntry<K, N, S> entry : snapshot) {
			while (null != entry) {
				result[pos++] = new Tuple3<>(entry.getKey(), entry.getNamespace(), entry.getState());
				entry = entry.next;
			}
		}
		Assert.assertEquals(mapSize, pos);
		return result;
	}

	@SuppressWarnings("unchecked")
	private Tuple3<Integer, Integer, ArrayList<Integer>>[] manualDeepDump(
			HashMap<Tuple2<Integer, Integer>,
					ArrayList<Integer>> map) {

		Tuple3<Integer, Integer, ArrayList<Integer>>[] result = new Tuple3[map.size()];
		int pos = 0;
		for (Map.Entry<Tuple2<Integer, Integer>, ArrayList<Integer>> entry : map.entrySet()) {
			Integer key = entry.getKey().f0;
			Integer namespace = entry.getKey().f1;
			result[pos++] = new Tuple3<>(key, namespace, new ArrayList<>(entry.getValue()));
		}
		return result;
	}

	private boolean deepCompare(
			Tuple3<Integer, Integer, ArrayList<Integer>>[] a,
			Tuple3<Integer, Integer, ArrayList<Integer>>[] b) {

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