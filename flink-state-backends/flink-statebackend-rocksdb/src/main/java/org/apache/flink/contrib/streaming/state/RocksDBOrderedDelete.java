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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.OrderedSetState;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * TODO.
 * - Use RocksDb iterator wrapper to check status.
 *
 * @param <T>
 */
public class RocksDBOrderedDelete<T> implements OrderedSetState<T> {

	@FunctionalInterface
	public interface PartialIterationConsumer<T> {
		boolean consume(T element);
	}

	private static final byte[] DUMMY_BYTES = "0".getBytes(ConfigConstants.DEFAULT_CHARSET);

//	/** The name of the element service. */
//	private final String serviceName;

	/** The total number of key groups. */
	private final int totalKeyGroups;

	/** The serialzier for the keys. */
	private final TypeSerializer<T> typeSerializer;

	/** The db where the elements are stored. */
	private final RocksDB db;
	private final ColumnFamilyHandle columnFamilyHandle;
	private final WriteOptions writeOptions;
	private final ReadOptions readOptions;

	/** The head elements of each group. */
	private final T[] groupHeadElements;

	/** The orders of the groups in heap. */
	private final int[] groupOrders;

	private final Comparator<T> comparator;

	private final KeyExtractorFunction<T> keyExtractor;

	private final int firstKeyGroup;

	/**
	 * The heap composed of the group indices. The first entry in the array is
	 * exactly the group with the earliest element.
	 */
	private final int[] groupIndexHeap;

	@SuppressWarnings("unchecked")
	public RocksDBOrderedDelete(
		KeyGroupRange keyGroupRange, int totalKeyGroups, TypeSerializer<T> typeSerializer, RocksDB db,
		ColumnFamilyHandle columnFamilyHandle, WriteOptions writeOptions, ReadOptions readOptions,
		Comparator<T> comparator, KeyExtractorFunction<T> keyExtractor) {
//		this.serviceName = serviceName;
		this.totalKeyGroups = totalKeyGroups;
		this.typeSerializer = typeSerializer;
		this.db = db;
		this.columnFamilyHandle = columnFamilyHandle;
		this.writeOptions = writeOptions;
		this.readOptions = readOptions;
		this.comparator = comparator;
		this.keyExtractor = keyExtractor;
		// Initialize the ordering of the groups with their indices.
		int startKeyGroup = keyGroupRange.getStartKeyGroup();
		int numKeyGroups = keyGroupRange.getNumberOfKeyGroups();

		this.firstKeyGroup = keyGroupRange.getStartKeyGroup();
		this.groupHeadElements = (T[]) new Object[numKeyGroups];
		this.groupOrders = new int[numKeyGroups];
		this.groupIndexHeap = new int[numKeyGroups];

		for (int i = 0; i < numKeyGroups; ++i) {
			groupOrders[i] = i;
			groupIndexHeap[i] = i;
		}
	}

	private int keyGroupToIndex(int keyGroup) {
		return keyGroup - firstKeyGroup;
	}

	@Nullable
	@Override
	public T poll() {
		return null;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public void clear() {

	}

	@Override
	public void addAll(@Nullable Collection<? extends T> toAdd) {

		if (toAdd == null) {
			return;
		}

		for (T element : toAdd) {
			add(element);
		}

//		T groupHeadElement = groupHeadElements.get(group);
//
//		for (T element : elements) {
//
//			insertDB(element);
//
//			if (compareElements(groupHeadElement, element) > 0) {
//				groupHeadElement = element;
//			}
//		}
//
//		updateGroupHeader(group, groupHeadElement);
	}

	@Nonnull
	@Override
	public Iterator<T> iterator() {
		return Collections.<T>emptyList().iterator();
	}

	/**
	 * Adds the given element into the heap.
	 *
	 * @param element The element to be added into the heap.
	 * @return <code>true</> only if the element was added as the new head. Otherwise it is unclear if the element was
	 * actually added as new and we signal <code>false</>. Caller can double-check using the (expensive)
	 * {@link #size()} call if a more accurate result is needed.
	 */
	@Override
	public boolean add(T element) {

		insertDB(element);

		T headElement = groupHeadElements[groupIndexHeap[0]];
		boolean isNewHead = compareElements(element, headElement) < 0;

		int group = computeKeyGroupForElement(element);
		final int groupIndex = keyGroupToIndex(group);
		T groupHeadElement = groupHeadElements[groupIndex];

		if (compareElements(element, groupHeadElement) < 0) {
			updateGroupHeader(groupIndex, element);
		}

		return isNewHead;
	}

	/**
	 * Removes the given element from the heap.
	 *
	 * @param element The element to be removed from the heap.
	 * @return <code>true</> only if the element is the head before the removal. Otherwise the result is unclear and we
	 * signal <code>false</>. Caller can double-check using the (expensive) {@link #size()} call if a more accurate
	 * result is needed.
	 */
	@Override
	public boolean remove(T element) {
		removeDB(element);

		T headElement = groupHeadElements[groupIndexHeap[0]];
		boolean isOldHead = element.equals(headElement);

		int group = computeKeyGroupForElement(element);
		T groupHeadElement = groupHeadElements[keyGroupToIndex(group)];

		if (element.equals(groupHeadElement)) {
			T newGroupHeadElement = electGroupHeader(group);
			updateGroupHeader(group, newGroupHeadElement);
		}

		return isOldHead;
	}

	/**
	 * Returns the first element in the heap.
	 *
	 * @return The first element in the heap.
	 */
	@Nullable
	@Override
	public T peek() {
		return groupHeadElements[groupIndexHeap[0]];
	}

	public void poll(PartialIterationConsumer<T> consumer) {
		while (true) {
			int group = groupIndexHeap[0];
			T groupHeadElement = groupHeadElements[keyGroupToIndex(group)];

			if (groupHeadElement == null || !consumer.consume(groupHeadElement)) {
				break;
			}

			T newGroupHeader = electGroupHeader(group, consumer);
			updateGroupHeader(group, newGroupHeader);
		}
	}

	/**
	 * Returns and removes the elements whose timestamps are smaller than or equal
	 * to the given timestamp.
	 *
	 * @param endInclusive The high endpoint (inclusive) of the timestamps of the retrieved elements.
	 * @return The elements whose timestamps are smaller than or equal to the given timestamp.
	 */
	List<T> poll(T endInclusive) {
		List<T> expiredElements = new ArrayList<>();

		while (true) {
			int groupIndex = groupIndexHeap[0];
			T groupHeadElement = groupHeadElements[groupIndex];
			if (groupHeadElement == null || comparator.compare(groupHeadElement, endInclusive) > 0) {
				break;
			}

			T newGroupHeader = electGroupHeader(groupIndex + firstKeyGroup, endInclusive, expiredElements);
			updateGroupHeader(groupIndex, newGroupHeader);
		}

		return expiredElements;
	}

	/**
	 * Returns the elements in the given key group.
	 *
	 * @param group The key group of the retrieved elements.
	 * @return The elements in the given key group.
	 */
	Set<T> getAll(int group) {

		Set<T> elements = new HashSet<>();

		try (RocksIteratorWrapper iterator = createNewRocksIterator()) {

			byte[] groupPrefixBytes = serializeGroupPrefix(group);
			iterator.seek(groupPrefixBytes);

			while (iterator.isValid()) {
				byte[] elementBytes = iterator.key();
				if (!isPrefixWith(elementBytes, groupPrefixBytes)) {
					break;
				}

				T element = deserializeElement(elementBytes);
				elements.add(element);

				iterator.next();
			}
		}

		return elements.isEmpty() ? null : elements;
	}

	/**
	 * Adds the elements into the given key group.
	 *
	 * @param group The key group into which the elements are added.
	 * @param elements The elements to be added.
	 */
	void addAll(int group, Iterable<T> elements) {

		T groupHeadElement = groupHeadElements[keyGroupToIndex(group)];

		for (T element : elements) {

			insertDB(element);

			if (compareElements(groupHeadElement, element) > 0) {
				groupHeadElement = element;
			}
		}

		updateGroupHeader(group, groupHeadElement);
	}


	//--------------------------------------------------------------------------

	private void insertDB(T element) {
		byte[] elementBytes = serializeElement(element);

		try {
			db.put(columnFamilyHandle, writeOptions, elementBytes, DUMMY_BYTES);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error while getting element from RocksDB.", e);
		}
	}

	private void removeDB(T element) {
		byte[] elementBytes = serializeElement(element);

		try {
			db.delete(columnFamilyHandle, writeOptions, elementBytes);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error while removing element from RocksDB.", e);
		}
	}

	/**
	 * Returns the head element of the given key group.
	 *
	 * @param group The group whose head element is to be retrieved.
	 * @return The head element of the given key group.
	 */
	private T electGroupHeader(int group) {
		return electGroupHeader(group, null, new ArrayList<>());
	}

	/**
	 * Returns the first element whose timestamp is larger than the {@code timestamp}
	 * and removes all the elements in the given group whose timestamps are smaller
	 * than or equal to the {@code timestamp}. All removed elements will be stored
	 * in {@code expiredElements}.
	 *
	 * @param group The group whose head element is to be retrieved.
	 * @param endInclusive The low endpoint (exclusive) of the elements in the group after the election.
	 * @param expiredElements The elements removed in the election.
	 * @return The first element in the given group after the election.
	 */
	private T electGroupHeader(
		int group,
		T endInclusive,
		List<T> expiredElements) {

		T groupHeadElement = null;
		List<T> groupExpiredElements = new ArrayList<>();

		try (RocksIteratorWrapper iterator = createNewRocksIterator()) {

			byte[] groupPrefixBytes = serializeGroupPrefix(group);
			iterator.seek(groupPrefixBytes);

			while (iterator.isValid()) {

				byte[] elementBytes = iterator.key();
				if (!isPrefixWith(elementBytes, groupPrefixBytes)) {
					break;
				}

				T element = deserializeElement(elementBytes);

				if (endInclusive != null && comparator.compare(element, endInclusive) <= 0) {
					groupExpiredElements.add(element);
					iterator.next();
				} else {
					groupHeadElement = element;
					break;
				}
			}
		}

		for (T expiredElement : groupExpiredElements) {
			removeDB(expiredElement);
		}

		expiredElements.addAll(groupExpiredElements);

		return groupHeadElement;
	}

	private T electGroupHeader(
		int group,
		PartialIterationConsumer<T> consumer) {

		T groupHeadElement = null;

		try (RocksIteratorWrapper iterator = createNewRocksIterator()) {

			byte[] groupPrefixBytes = serializeGroupPrefix(group);
			iterator.seek(groupPrefixBytes);

			while (iterator.isValid()) {

				byte[] elementBytes = iterator.key();
				if (!isPrefixWith(elementBytes, groupPrefixBytes)) {
					break;
				}

				T element = deserializeElement(elementBytes);

				if (consumer.consume(element)) {
					iterator.next();
					removeDB(element);
				} else {
					groupHeadElement = element;
					break;
				}
			}
		}

		return groupHeadElement;
	}

	/**
	 * Updates the head element of the given groupIndex. The heap will be adjusted
	 * accordingly to the update.
	 *
	 * @param groupIndex The groupIndex whose head element is updated.
	 * @param element The new head element of the groupIndex.
	 */
	private void updateGroupHeader(int groupIndex, T element) {

		groupHeadElements[groupIndex] =  element;

		int currentOrder = groupOrders[groupIndex];

		// Walk up and swap with the parent if the parent's timestamp is larger.
		while (currentOrder > 0) {
			int currentGroupIndex = groupIndexHeap[currentOrder];
			T currentElement = groupHeadElements[currentGroupIndex];

			int parentOrder = getParentOrder(currentOrder);
			int parentGroup = groupIndexHeap[parentOrder];
			T parentElement = groupHeadElements[parentGroup];

			if (compareElements(currentElement, parentElement) >= 0) {
				break;
			} else {
				groupIndexHeap[currentOrder] = parentGroup;
				groupOrders[parentGroup] = currentOrder;

				groupIndexHeap[parentOrder] = currentGroupIndex;
				groupOrders[currentGroupIndex] =  parentOrder;

				currentOrder = parentOrder;
			}
		}

		// Walk down and swap with the child if the child's timestamp is smaller.
		while (currentOrder < groupOrders.length) {
			int currentGroup = groupIndexHeap[currentOrder];
			T currentElement = groupHeadElements[currentGroup];

			int leftChildOrder = getLeftChildOrder(currentOrder);
			int leftChildGroup = leftChildOrder < groupIndexHeap.length ? groupIndexHeap[leftChildOrder] : -1;
			T leftChildElement = groupHeadElements[leftChildGroup];

			int rightChildOrder = getRightChildOrder(currentOrder);
			int rightChildGroup = rightChildOrder < groupIndexHeap.length ? groupIndexHeap[rightChildOrder] : -1;
			T rightChildElement = groupHeadElements[rightChildGroup];

			if (compareElements(currentElement, leftChildElement) <= 0 && compareElements(currentElement, rightChildElement) <= 0) {
				break;
			} else {
				if (compareElements(leftChildElement, rightChildElement) < 0) {

					groupIndexHeap[currentOrder] = leftChildGroup;
					groupOrders[leftChildGroup] = currentOrder;

					groupIndexHeap[leftChildOrder] = currentGroup;
					groupOrders[currentGroup] = leftChildOrder;

					currentOrder = leftChildOrder;
				} else {

					groupIndexHeap[currentOrder] = rightChildGroup;
					groupOrders[rightChildGroup] = currentOrder;

					groupIndexHeap[rightChildOrder] = currentGroup;
					groupOrders[currentGroup] = rightChildOrder;

					currentOrder = rightChildOrder;
				}
			}
		}
	}

//	int numElements(N namespace) {
//		int count = 0;
//
//		try (RocksIterator iterator = db.newIterator(columnFamilyHandle)) {
//			byte[] servicePrefixBytes = serializeServicePrefix();
//			iterator.seek(servicePrefixBytes);
//
//			while (iterator.isValid()) {
//
//				byte[] elementBytes = iterator.key();
//				if (!isPrefixWith(elementBytes, servicePrefixBytes)) {
//					break;
//				}
//
//				InternalElement<K, N> element = deserializeElement(elementBytes);
//				if (namespace == null || Objects.equals(namespace, element.getNamespace())) {
//					count++;
//				}
//
//				iterator.next();
//			}
//		}
//
//		return count;
//	}

	//--------------------------------------------------------------------------
	// Serialization Methods
	//--------------------------------------------------------------------------

	private RocksIteratorWrapper createNewRocksIterator() {
		return new RocksIteratorWrapper(db.newIterator(columnFamilyHandle, readOptions));
	}

//	private byte[] serializeServicePrefix() {
//		try {
//			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
//			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
//
//			StringSerializer.INSTANCE.serialize(serviceName, outputView);
//
//			return outputStream.toByteArray();
//		} catch (IOException e) {
//			throw new FlinkRuntimeException("Error while serializing the prefix with service name.", e);
//		}
//	}

	private byte[] serializeGroupPrefix(int keyGroup) {
		try {
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);

			IntSerializer.INSTANCE.serialize(keyGroup, outputView);

			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing the prefix with key group.", e);
		}
	}

	private byte[] serializeElement(T element, int elementKeyGroup) {
		try {
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			IntSerializer.INSTANCE.serialize(elementKeyGroup, outputView);
			typeSerializer.serialize(element, outputView);
			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing the element.", e);
		}
	}

	private byte[] serializeElement(T element) {
		int group = computeKeyGroupForElement(element);
		return serializeElement(element, group);
	}

	private T deserializeElement(byte[] bytes) {
		try {
			ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(bytes);
			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
			StringSerializer.INSTANCE.deserialize(inputView);
			IntSerializer.INSTANCE.deserialize(inputView);
			return typeSerializer.deserialize(inputView);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while deserializing the element.", e);
		}
	}

	//--------------------------------------------------------------------------
	// Auxiliary Methods
	//--------------------------------------------------------------------------

	private int computeKeyGroupForElement(T element) {
		return KeyGroupRangeAssignment.assignToKeyGroup(keyExtractor.extractKeyFromElement(element), totalKeyGroups);
	}

	private static int getParentOrder(int order) {
		return (order - 1) / 2;
	}

	private static int getLeftChildOrder(int order) {
		return order * 2 + 1;
	}

	private static int getRightChildOrder(int order) {
		return order * 2 + 2;
	}

	private static boolean isPrefixWith(byte[] bytes, byte[] prefixBytes) {
		if (bytes.length < prefixBytes.length) {
			return false;
		}

		for (int i = 0; i < prefixBytes.length; ++i) {
			if (bytes[i] != prefixBytes[i]) {
				return false;
			}
		}

		return true;
	}

	private int compareElements(T leftElement, T rightElement) {
		if (leftElement == null) {
			return (rightElement == null ? 0 : 1);
		} else {
			return (rightElement == null ? -1 : comparator.compare(leftElement, rightElement));
		}
	}
}
