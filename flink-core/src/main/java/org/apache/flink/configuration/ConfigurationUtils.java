/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.configuration;

import javax.annotation.Nonnull;

import java.io.File;

/**
 * Utility class for {@link Configuration} related helper functions.
 */
public class ConfigurationUtils {

	/**
	 * Extracts the task manager directories for temporary files as defined by
	 * {@link org.apache.flink.configuration.CoreOptions#TMP_DIRS}.
	 *
	 * @param configuration configuration object
	 * @return array of configured directories (in order)
	 */
	public static String[] parseTempDirectories(Configuration configuration) {
		return splitPaths(configuration.getString(CoreOptions.TMP_DIRS));
	}

	/**
	 * Extracts the local state directories  as defined by
	 * {@link org.apache.flink.configuration.ConfigConstants#TASK_MANAGER_LOCAL_STATE_ROOT_DIR_KEY}.
	 *
	 * @param configuration configuration object
	 * @return array of configured directories (in order)
	 */
	public static String[] parseLocalStateDirectories(Configuration configuration) {
		String configValue = configuration.getString(
			ConfigConstants.TASK_MANAGER_LOCAL_STATE_ROOT_DIR_KEY,
			configuration.getString(CoreOptions.TMP_DIRS));
		return splitPaths(configValue);
	}

	private static String[] splitPaths(@Nonnull String separatedPaths) {
		return separatedPaths.split(",|" + File.pathSeparator);
	}

	// Make sure that we cannot instantiate this class
	private ConfigurationUtils() {
	}
}
