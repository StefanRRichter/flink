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

package org.apache.flink.api.common.state.ttl;

import org.apache.flink.api.common.state.ListState;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class TtlListStateDecorator<T> implements ListState<T> {
	private final ListState<TtlValue<T>> originalState;
	private final TtlConfig config;
	private final TtlTimeProvider timeProvider;

	TtlListStateDecorator(ListState<TtlValue<T>> originalState,
						  TtlConfig config,
						  TtlTimeProvider timeProvider) {
		this.originalState = originalState;
		this.config = config;
		this.timeProvider = timeProvider;
	}

	@Override
	public void update(List<T> values) throws Exception {
		originalState.update(withTs(values));
	}

	@Override
	public void addAll(List<T> values) throws Exception {
		originalState.addAll(withTs(values));
	}

	private List<TtlValue<T>> withTs(List<T> values) {
		long expirationTimestamp = newExpirationTimestamp();
		return values.stream()
			.map(v -> new TtlValue<>(v, expirationTimestamp))
			.collect(Collectors.toList());
	}

	@Override
	public Iterable<T> get() throws Exception {
		Iterable<TtlValue<T>> ttlValue = originalState.get();
		Stream<TtlValue<T>> unexpired = StreamSupport
			.stream(ttlValue.spliterator(), false)
			.filter(v -> v.getExpirationTimestamp() > timeProvider.currentTimestamp());
		if (config.getTtlUpdateType() == TtlUpdateType.OnReadAndWrite) {
			long newExpirationTimestamp = newExpirationTimestamp();
			List<TtlValue<T>> withUpdatedTs = unexpired
				.map(v -> v.withNewExpirationTimestamp(newExpirationTimestamp))
				.collect(Collectors.toList());
			originalState.update(withUpdatedTs);
			unexpired = withUpdatedTs.stream();
		}
		return unexpired.map(TtlValue::getUserValue)::iterator;
	}

	@Override
	public void add(T value) throws Exception {
		originalState.add(new TtlValue<>(value, newExpirationTimestamp()));
	}

	@Override
	public void clear() {
		originalState.clear();
	}

	private long newExpirationTimestamp() {
		return timeProvider.currentTimestamp() + config.getTtl().toMilliseconds();
	}
}
