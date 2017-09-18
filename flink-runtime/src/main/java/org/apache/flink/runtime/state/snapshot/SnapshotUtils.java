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

package org.apache.flink.runtime.state.snapshot;

import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

public class SnapshotUtils {

	public static <T extends StateSnapshot> T findPrimarySnapshot(
		Collection<? extends T> snapshots) {

		if (snapshots == null) {
			return null;
		}

		T result = null;

		for (T snapshot : snapshots) {
			SnapshotMetaData snapshotMetaData = snapshot.getMetaData();
			if (SnapshotMetaData.Ownership.JobManager.equals(snapshotMetaData.getOwnership())) {
				Preconditions.checkState(result == null, "More than one primary state snapshot!");
				result = snapshot;
			}
		}

		return result;
	}

//	public static <T extends StateObject> Collection<T> findPrimarySnapshotHandles(
//		Collection<? extends AbstractHandleBasedSnapshot<T>> snapshots) {
//
//		AbstractHandleBasedSnapshot<T> primarySnapshot = findPrimarySnapshot(snapshots);
//
//		return primarySnapshot != null ? primarySnapshot.stateObjects : null;
//	}
//
//	public static <T extends StateObject> T findPrimarySnapshotSingletonHandle(
//		Collection<? extends AbstractHandleBasedSnapshot<T>> snapshots) {
//
//		Collection<T> primarySnapshot = findPrimarySnapshotHandles(snapshots);
//
//		T result = null;
//
//		if (primarySnapshot != null) {
//			for (T handle : primarySnapshot) {
//				Preconditions.checkState(result == null, "More than one state handle!");
//				result = handle;
//			}
//		}
//		return result;
//	}

	public static KeyedStateHandleSnapshot toPrimaryKeyedSnapshot(KeyedStateHandle ksh) {

		if (ksh == null) {
			return null;
		}

		return new KeyedStateHandleSnapshot(SnapshotMetaData.createPrimarySnapshotMetaData(), ksh);
	}

	public static OperatorStateHandleSnapshot toPrimaryOperatorSnapshot(OperatorStateHandle osh) {

		if (osh == null) {
			return null;
		}

		return new OperatorStateHandleSnapshot(SnapshotMetaData.createPrimarySnapshotMetaData(), osh);
	}

	public static KeyedStateHandleSnapshot toPrimaryKeyedSnapshot(Collection<KeyedStateHandle> ksh) {

		if (ksh == null) {
			return null;
		}

		return new KeyedStateHandleSnapshot(SnapshotMetaData.createPrimarySnapshotMetaData(), ksh);
	}

	public static OperatorStateHandleSnapshot toPrimaryOperatorSnapshot(Collection<OperatorStateHandle> osh) {

		if (osh == null) {
			return null;
		}

		return new OperatorStateHandleSnapshot(SnapshotMetaData.createPrimarySnapshotMetaData(), osh);
	}
}
