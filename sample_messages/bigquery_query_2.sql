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

# [START metric_export_bigquery_sql_2]
SELECT
  EXTRACT(DATE  FROM point.interval.end_time) AS extract_date,
  sum(point.value.int64_value) as query_cnt
FROM
  `sage-facet-201016.metric_export.sd_metrics_export`
CROSS JOIN
  UNNEST(resource.labels) AS resource_labels
WHERE
   point.interval.end_time > TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY))
  AND  point.interval.end_time <= CURRENT_TIMESTAMP
  and metric.type = 'bigquery.googleapis.com/query/count'
  AND resource_labels.key = "project_id"
  AND resource_labels.value = "sage-facet-201016"
group by extract_date
order by extract_date
# [END metric_export_bigquery_sql_2]