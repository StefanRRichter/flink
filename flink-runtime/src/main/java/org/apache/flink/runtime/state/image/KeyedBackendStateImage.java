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

package org.apache.flink.runtime.state.image;

import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.StateImageMetaData;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Method;

/**
 * TODO introduce RestoreException ?
 */
public abstract class KeyedBackendStateImage<T extends AbstractKeyedStateBackend<?>> implements StateObject {

	private static final long serialVersionUID = 1L;

	protected final StateImageMetaData stateImageMetaData;

	protected KeyedBackendStateImage(StateImageMetaData stateImageMetaData) {
		this.stateImageMetaData = Preconditions.checkNotNull(stateImageMetaData);
	}

	public void restore(AbstractKeyedStateBackend<?> backend) throws Exception {
		Method m = this.getClass().getDeclaredMethod("doRestore", backend.getClass());
		m.invoke(this, backend);
	}

	abstract protected void doRestore(T backend) throws Exception;

	public StateImageMetaData getStateImageMetaData() {
		return stateImageMetaData;
	}
}

//
//abstract class Restorer<B extends AbstractKeyedStateBackend<?>, I extends KeyedBackendStateImage> {
//
//	protected final Class<? extends B> backendClass;
//	protected final Class<? extends I> imageClass;
//
//	protected Restorer(Class<? extends B> backendClass, Class<? extends I> imageClass) {
//		this.backendClass = backendClass;
//		this.imageClass = imageClass;
//	}
//
//	void restore(AbstractKeyedStateBackend<?> backend, KeyedBackendStateImage image) {
//		restoreInternal(getBackendClass().cast(backend), getImageClass().cast(image));
//	}
//
//	abstract void restoreInternal(B b, I i);
//
//	public Class<? extends B> getBackendClass() {
//		return backendClass;
//	}
//
//	public Class<? extends I> getImageClass() {
//		return imageClass;
//	}
//}
//
//class XXX {
//
//	Map<Tuple2<Class<?>, Class<?>>, Restorer<?, ?>> mappings;
//
//	<B extends AbstractKeyedStateBackend<?>, I extends KeyedBackendStateImage> void put(
//		Class<? extends B> cB,
//		Class<? extends I> cI,
//		Restorer<B, I> r) {
//
//		mappings.put(new Tuple2<>(cB, cI), r);
//	}
//
//	void restore(AbstractKeyedStateBackend b, KeyedBackendStateImage i) {
//		mappings.get(new Tuple2<>(b.getClass(), i.getClass())).restore(b, i);
//	}
//}

