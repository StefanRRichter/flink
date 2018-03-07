#!/usr/bin/env bash

################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

source "$(dirname "$0")"/common.sh

parallelism=4

TEST_PROGRAM_JAR=$TEST_INFRA_DIR/../../flink-end-to-end-tests/target/flink-end-to-end-tests_2.11-1.6-SNAPSHOT.jar

start_cluster

tm_watchdog ${parallelism} &
watchdogPid=$!

#for slave in {1..4};
#do
#    "${FLINK_BIN_DIR}"/taskmanager.sh "start"
#done

$FLINK_DIR/bin/flink run -c org.apache.flink.streaming.tests.SimpleStatefulJob -p ${parallelism} $TEST_PROGRAM_JAR \
--resolve-order parent-first --checkpointDir file:///Users/stefan/test-tmp --output $TEST_DATA_DIR/out/simple_out_pf \
--checkpointInterval 300 --maxAttempts 5 --parallelism ${parallelism}

#QUERY_RESULT=$(curl "http://localhost:9065/jobs/overview" 2> /dev/null || true)

kill ${watchdogPid}
tm_all_kill

#http://localhost:9065/jobs/<jobid>/vertices/<vertexid>/taskmanagers
#http://localhost:9065/jobs/<jobid>/vertices/<vertexid>/taskmanagers
