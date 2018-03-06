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

TEST_PROGRAM_JAR=$TEST_INFRA_DIR/../../flink-end-to-end-tests/target/ClassLoaderTestProgram.jar

start_cluster

$FLINK_DIR/bin/flink run -p 1 $TEST_PROGRAM_JAR --resolve-order parent-first --checkpointDir file:///Users/stefan/test-tmp --output $TEST_DATA_DIR/out/simple_out_pf

QUERY_RESULT=$(curl "http://localhost:9065/jobs/overview" 2> /dev/null || true)

#http://localhost:9065/jobs/<jobid>/vertices/<vertexid>/taskmanagers
#http://localhost:9065/jobs/<jobid>/vertices/<vertexid>/taskmanagers
