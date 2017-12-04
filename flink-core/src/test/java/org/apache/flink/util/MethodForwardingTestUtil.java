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

package org.apache.flink.util;

import org.mockito.Mockito;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

/**
 * Helper class with a method that attempt to automatically test method forwarding between a delegate and a wrapper.
 */
public class MethodForwardingTestUtil {

	/**
	 * This is a best effort automatic test for method forwarding between a delegate and its wrapper, where the wrapper
	 * class is a subtype of the delegate. This ignores methods that are inherited from Object.
	 *
	 * @param delegateClass the class for the delegate.
	 * @param wrapperFactory factory that produces a wrapper from a delegate.
	 * @param <D> type of the delegate
	 * @param <W> type of the wrapper
	 */
	public static <D, W> void testMethodForwarding(
		Class<D> delegateClass,
		Function<D, W> wrapperFactory) {
		testMethodForwarding(delegateClass, wrapperFactory, Collections.emptySet());
	}

	/**
	 * This is a best effort automatic test for method forwarding between a delegate and its wrapper, where the wrapper
	 * class is a subtype of the delegate. Methods can be remapped in case that the implementation does not call the
	 * original method. Remapping to null skips the method. This ignores methods that are inherited from Object.
	 *
	 * @param delegateClass the class for the delegate.
	 * @param wrapperFactory factory that produces a wrapper from a delegate.
	 * @param skipMethodSet set of methods to ignore.
	 * @param <D> type of the delegate
	 * @param <W> type of the wrapper
	 */
	public static <D, W> void testMethodForwarding(
		Class<D> delegateClass,
		Function<D, W> wrapperFactory,
		Set<Method> skipMethodSet) {

		Preconditions.checkNotNull(delegateClass);
		Preconditions.checkNotNull(wrapperFactory);
		Preconditions.checkNotNull(skipMethodSet);

		D delegate = spy(delegateClass);
		W wrapper = wrapperFactory.apply(delegate);

		// ensure that wrapper is a subtype of delegate
		Preconditions.checkArgument(delegateClass.isAssignableFrom(wrapper.getClass()));

		for (Method delegateMethod : delegateClass.getMethods()) {

			if (checkSkipMethodForwardCheck(delegateMethod, skipMethodSet)) {
				continue;
			}

			try {
				// find the correct method to substitute the bridge for erased generic types.
				// if this doesn't work, the user need to exclude the method and write an additional test.
				Method wrapperMethod = wrapper.getClass().getDeclaredMethod(
					delegateMethod.getName(),
					delegateMethod.getParameterTypes());

				// things get a bit fuzzy here, best effort to find a match but this might end up with a wrong method.
				if (wrapperMethod.isBridge()) {
					for (Method method : wrapper.getClass().getDeclaredMethods()) {
						if (!method.isBridge()
							&& method.getName().equals(wrapperMethod.getName())
							&& method.getParameterCount() == wrapperMethod.getParameterCount()) {
							wrapperMethod = method;
							break;
						}
					}
				}

				Class<?>[] parameterTypes = wrapperMethod.getParameterTypes();
				Object[] arguments = new Object[parameterTypes.length];
				for (int j = 0; j < arguments.length; j++) {
					Class<?> parameterType = parameterTypes[j];
					if (parameterType.isArray()) {
						arguments[j] = Array.newInstance(parameterType.getComponentType(), 0);
					} else if (parameterType.isPrimitive()) {
						arguments[j] = 0;
					} else {
						arguments[j] = Mockito.mock(parameterType);
					}
				}

				wrapperMethod.invoke(wrapper, arguments);
				delegateMethod.invoke(Mockito.verify(delegate, Mockito.times(1)), arguments);
				reset(delegate);
			} catch (Exception ex) {
				ex.printStackTrace();
				fail("Forwarding test failed: " + ex.getMessage());
			}
		}
	}

	/**
	 * Test if this method should be skipped in our check for proper forwarding, e.g. because it is just a bridge.
	 */
	private static boolean checkSkipMethodForwardCheck(Method delegateMethod, Set<Method> skipMethods) {

		if (delegateMethod.isBridge()
			|| delegateMethod.isDefault()
			|| skipMethods.contains(delegateMethod)) {
			return true;
		}

		// skip methods declared in Object (Mockito doesn't like them)
		try {
			Object.class.getMethod(delegateMethod.getName(), delegateMethod.getParameterTypes());
			return true;
		} catch (Exception ignore) {
		}
		return false;
	}
}
