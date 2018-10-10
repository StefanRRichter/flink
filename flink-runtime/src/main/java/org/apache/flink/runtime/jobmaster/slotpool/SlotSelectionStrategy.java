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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotInfo;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;

/**
 * TODO
 */
public interface SlotSelectionStrategy {

	@Nonnull
	SlotInfoAndLocality selectBestSlotForProfile(
		@Nonnull List<SlotInfo> availableSlots,
		@Nonnull SlotProfile slotProfile);


	/**
	 * TODO
	 */
	class SlotInfoAndLocality {

		@Nullable
		private final SlotInfo slotInfo;

		@Nonnull
		private final Locality locality;

		private SlotInfoAndLocality(@Nullable SlotInfo slotInfo, @Nonnull Locality locality) {
			this.slotInfo = slotInfo;
			this.locality = locality;
		}

		@Nullable
		public SlotInfo getSlotInfo() {
			return slotInfo;
		}

		@Nonnull
		public Locality getLocality() {
			return locality;
		}

		public static SlotInfoAndLocality of(@Nullable SlotInfo slotInfo, @Nonnull Locality locality) {
			return new SlotInfoAndLocality(slotInfo, locality);
		}
	}
}
