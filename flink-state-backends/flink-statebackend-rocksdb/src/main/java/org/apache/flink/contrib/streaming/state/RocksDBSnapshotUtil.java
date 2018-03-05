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

class RocksDBSnapshotUtil {

	/** File suffix of sstable files. */
	static final String SST_FILE_SUFFIX = ".sst";

	static final int FIRST_BIT_IN_BYTE_MASK = 0x80;
	
	static final int END_OF_KEY_GROUP_MARK = 0xFFFF;

	static void setMetaDataFollowsFlagInKey(byte[] key) {
		key[0] |= FIRST_BIT_IN_BYTE_MASK;
	}

	static void clearMetaDataFollowsFlag(byte[] key) {
		key[0] &= (~FIRST_BIT_IN_BYTE_MASK);
	}

	static boolean hasMetaDataFollowsFlag(byte[] key) {
		return 0 != (key[0] & FIRST_BIT_IN_BYTE_MASK);
	}

	private RocksDBSnapshotUtil() {
		throw new AssertionError();
	}
}
