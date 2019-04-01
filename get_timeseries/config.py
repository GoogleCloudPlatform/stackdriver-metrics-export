#!/usr/bin/env python

# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PUBSUB_TOPIC="write_metrics"
AGGREGATION_ALIGNMENT_PERIOD="3600s"
GROUP_BY_STRING="metric.labels.key"
PAGE_SIZE=500
PUBSUB_VERIFICATION_TOKEN = '16b2ecfb-7734-48b9-817d-4ac8bd623c87'
BIGQUERY_DATASET='metric_export'
BIGQUERY_STATS_TABLE='sd_metrics_stats'
WRITE_BQ_STATS_FLAG=True

GAUGE="GAUGE"
DELTA="DELTA"
CUMULATIVE="CUMULATIVE"

BOOL="BOOL"
INT64="INT64"
DOUBLE="DOUBLE"
STRING="STRING"
DISTRIBUTION="DISTRIBUTION"

ALIGN_DELTA="ALIGN_DELTA"
ALIGN_FRACTION_TRUE="ALIGN_FRACTION_TRUE"
ALIGN_SUM="ALIGN_SUM"
ALIGN_COUNT="ALIGN_COUNT"
ALIGN_NONE="ALIGN_NONE"
REDUCE_FRACTION_TRUE="REDUCE_FRACTION_TRUE"
REDUCE_MEAN="REDUCE_MEAN"
REDUCE_SUM="REDUCE_SUM"
REDUCE_COUNT="REDUCE_COUNT"
REDUCE_NONE="REDUCE_NONE"