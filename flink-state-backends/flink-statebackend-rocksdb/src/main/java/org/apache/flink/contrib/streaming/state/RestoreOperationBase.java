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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyedBackendStateMetaInfo;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.util.Map;

public abstract class RestoreOperationBase<K> {

	protected final RocksDB db;

	/** The DB options from the options factory. */
	protected final DBOptions dbOptions;

	/** The column family options from the options factory. */
	protected final ColumnFamilyOptions columnOptions;

	protected final WriteOptions writeOptions;

	protected final CloseableRegistry cancelStreamRegistry;

	protected final KeyGroupRange keyGroupRange;

	protected final ClassLoader userCodeClassLoader;

	protected final TypeSerializer<K> keySerializer;
}
