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

# [START metric_export_bigquery_sql_1]
SELECT
  metric.type AS metric_type,
  EXTRACT(DATE  FROM point.INTERVAL.start_time) AS extract_date,
  MAX(point.value.distribution_value.mean) AS max_mean,
  MIN(point.value.distribution_value.mean) AS min_mean,
  AVG(point.value.distribution_value.mean) AS avg_mean
FROM
  `sage-facet-201016.metric_export.sd_metrics_export`
CROSS JOIN
  UNNEST(resource.labels) AS resource_labels
WHERE
   point.interval.end_time > TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY))
  AND  point.interval.end_time <= CURRENT_TIMESTAMP
  AND metric.type = 'appengine.googleapis.com/http/server/response_latencies'
  AND resource_labels.key = "project_id"
  AND resource_labels.value = "sage-facet-201016"
GROUP BY
  metric_type,
  extract_date
ORDER BY
  extract_date
# [END metric_export_bigquery_sql_1]
