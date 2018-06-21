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

/**
 * Interface for objects that can be managed by a {@link HeapOrderedSet}. Such an object can only be contained in at
 * most one {@link HeapOrderedSet} at a time.
 */
public interface HeapOrderedSetElement {

	/**
	 * The index that indicates that a {@link HeapOrderedSetElement} object is not contained in any
	 * {@link HeapOrderedSet}.
	 */
	int NOT_CONTAINED = Integer.MIN_VALUE;

	/**
	 * Returns the current index of this object in the internal array of {@link HeapOrderedSet}.
	 */
	int getManagedIndex();

	/**
	 * Sets the current index of this object in the {@link HeapOrderedSet} and should only be called by the owning
	 * {@link HeapOrderedSet}.
	 *
	 * @param newIndex the new index in the timer heap.
	 */
	void setManagedIndex(int newIndex);
}
