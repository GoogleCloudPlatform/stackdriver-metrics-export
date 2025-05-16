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

import logging
import json
from googleapiclient.discovery import build
import unittest
import config

class AppTest(unittest.TestCase):

    def setUp(self):
        """ Set-up the app
        """
        self.batch_id = config.BATCH_ID
        self.project_id = config.PROJECT_ID

    def get_list_metrics_output_stats(self, batch_id):
        query = "SELECT count(message_id)" \
            "FROM " \
            "`metric_export.sd_metrics_stats` " \
            "WHERE " \
            "batch_id=\"{}\" AND app_name=\"list_metrics\" AND msg_written_cnt=1".format(batch_id)

        response = self.query_bigquery(query)
        job_ref = response["jobReference"]
        results = self.get_query_results_bigquery(job_ref)
        return results

    def get_list_metrics_num_full_recs(self, batch_id):
        query = "SELECT count(message_id)" \
            "FROM " \
            "`metric_export.sd_metrics_stats` " \
            "WHERE " \
            "batch_id=\"{}\" AND app_name=\"list_metrics\"".format(batch_id)

        response = self.query_bigquery(query)
        job_ref = response["jobReference"]
        results = self.get_query_results_bigquery(job_ref)
        return results

    def get_get_timeseries_output_stats(self, batch_id):
        query = "SELECT count(t.message_id) "\
            "FROM (SELECT message_id " \
            "FROM " \
            "`metric_export.sd_metrics_stats` " \
            "WHERE " \
            "batch_id=\"{}\" AND app_name=\"get_timeseries\"" \
            "group by message_id) t ".format(batch_id)

        response = self.query_bigquery(query)
        job_ref = response["jobReference"]
        results = self.get_query_results_bigquery(job_ref)
        return results

    def get_stats(self, batch_id, app_name):
        query = "SELECT count(message_id)" \
            "FROM " \
            "`metric_export.sd_metrics_stats` " \
            "WHERE " \
            "batch_id=\"{}\" AND app_name=\"{}\" AND msg_written_cnt>=1".format(batch_id, app_name)

        response = self.query_bigquery(query)
        job_ref = response["jobReference"]
        results = self.get_query_results_bigquery(job_ref)
        return results

    def get_metrics_export_cnt(self, batch_id):
        query = "SELECT count(metric)" \
            "FROM " \
            "`metric_export.sd_metrics_export_fin` " \
            "WHERE " \
            "batch_id=\"{}\"".format(batch_id)

        response = self.query_bigquery(query)
        job_ref = response["jobReference"]
        results = self.get_query_results_bigquery(job_ref)
        return results

    def get_write_metrics_sum_recs_written(self, batch_id, app_name):
        query = "SELECT sum(msg_written_cnt)" \
            "FROM " \
            "`metric_export.sd_metrics_stats` " \
            "WHERE " \
            "batch_id=\"{}\" AND app_name=\"{}\"".format(batch_id, app_name)

        response = self.query_bigquery(query)
        job_ref = response["jobReference"]
        results = self.get_query_results_bigquery(job_ref)
        return results

    def query_bigquery(self, query):

        bigquery = build('bigquery', 'v2',cache_discovery=True)

        body = {
            "query": query,
            "useLegacySql": "false"
        }
        logging.debug('body: {}'.format(json.dumps(body,sort_keys=True, indent=4)))
        print('body: {}'.format(json.dumps(body,sort_keys=True, indent=4)))
        response = bigquery.jobs().query(
            projectId=self.project_id,
            body=body
        ).execute()

        logging.debug("BigQuery said... = {}".format(response))
        print("BigQuery said... = {}".format(response))

        return response

    def get_query_results_bigquery(self, job_ref):
        bigquery = build('bigquery', 'v2', cache_discovery=True)

        response = bigquery.jobs().getQueryResults(
            projectId=self.project_id,
            jobId=job_ref["jobId"]
        ).execute()

        logging.debug("BigQuery said... = {}".format(response))
        print("BigQuery said... = {}".format(json.dumps(response,sort_keys=True, indent=4)))

        return response

    def get_metric_descriptors_cnt(self):
        metrics_count_from_api = 0

        next_page_token = ""
        while True:
            metric_list = self.get_metrics(next_page_token)
            metrics_count_from_api += len(metric_list['metricDescriptors'])

            if "nextPageToken" in metric_list:
                next_page_token = metric_list["nextPageToken"]
            else:
                break
        return metrics_count_from_api

    def get_metrics(self, next_page_token, filter_str=""):
        service = build('monitoring', 'v3', cache_discovery=True)
        project_name = 'projects/{project_id}'.format(
            project_id=self.project_id
        )

        metrics = service.projects().metricDescriptors().list(
            name=project_name,
            pageToken=next_page_token,
            filter=filter_str
        ).execute()

        logging.debug("response is {}".format(json.dumps(metrics, sort_keys=True, indent=4)))
        return metrics

    def test_1(self):
        """
        test 1. The number of messages written to pubsub from list_metrics
        should match the number of messages received by get_timeseries
        """
        response = self.get_list_metrics_output_stats(self.batch_id)
        # print "response: {}".format(response)
        list_metric_row_cnt = response["rows"][0]["f"][0]["v"]
        print("list_metric_row_cnt: {}".format(list_metric_row_cnt))

        response = self.get_get_timeseries_output_stats(self.batch_id)
        # print "response: {}".format(response)
        get_timeseries_row_cnt = response["rows"][0]["f"][0]["v"]
        print("get_timeseries_row_cnt: {}".format(get_timeseries_row_cnt))

        assert list_metric_row_cnt == get_timeseries_row_cnt, \
            "Failed #1: The # of records written from list_metrics doesn't " \
            "match the # of records received by get_timeseries"

    def test_2(self):
        """
        test 2. The number of messages written to pubsub from get_timeseries
        should match the number of messages received by write_metrics
        """
        response = self.get_stats(self.batch_id,"get_timeseries")
        # print "response: {}".format(response)
        get_timeseries_row_cnt = response["rows"][0]["f"][0]["v"]
        print("get_timeseries_row_cnt: {}".format(get_timeseries_row_cnt))

        response = self.get_stats(self.batch_id,"write_metrics")
        # print "response: {}".format(response)
        write_metrics_row_cnt = response["rows"][0]["f"][0]["v"]
        print("write_metrics_row_cnt: {}".format(write_metrics_row_cnt))

        assert get_timeseries_row_cnt == write_metrics_row_cnt, \
            "Failed #2: The # of records written from get_timeseries doesn't " \
            "match the # of records received by write_metrics "\
            "write_metrics_row_cnt:{}, get_timeseries_row_cnt:{}"\
            .format(write_metrics_row_cnt, get_timeseries_row_cnt)

    def test_3(self):
        """
        test 3. The number of messages written to BigQuery from write_metrics
        should match the actual number of messages in the BigQuery table
        """
        response = self.get_write_metrics_sum_recs_written(self.batch_id,"write_metrics")
        # print "response: {}".format(response)
        write_metrics_row_cnt = response["rows"][0]["f"][0]["v"]
        if write_metrics_row_cnt is None:
            write_metrics_row_cnt = 0
        else:
            write_metrics_row_cnt = int(write_metrics_row_cnt)
        print("write_metrics_row_cnt: {}".format(write_metrics_row_cnt))

        response = self.get_metrics_export_cnt(self.batch_id)
        metrics_bq_row_cnt = response["rows"][0]["f"][0]["v"]
        if metrics_bq_row_cnt is None:
            metrics_bq_row_cnt = 0
        else:
            metrics_bq_row_cnt = int(metrics_bq_row_cnt)
        print("metrics_bq_row_cnt: {}".format(metrics_bq_row_cnt))

        assert write_metrics_row_cnt == metrics_bq_row_cnt, \
            "Failed #3: The # of records written from write_metrics doesn't " \
            "match the # of records received in BigQuery "\
            "write_metrics_row_cnt: {}, metrics_bq_row_cnt:{}"\
            .format(write_metrics_row_cnt, metrics_bq_row_cnt)

    def test_4(self):
        """
        test 4. The number of messages written to BigQuery from list_metrics
        should match the actual number of messages from the Monitoring API
        """
        metric_descriptors_cnt = self.get_metric_descriptors_cnt()
        response = self.get_list_metrics_num_full_recs(self.batch_id)
        bq_metrics_cnt = int(response["rows"][0]["f"][0]["v"])
        print("bq_metrics_cnt: {}, metric_descriptors_cnt: {}".format(bq_metrics_cnt, metric_descriptors_cnt))

        assert metric_descriptors_cnt == bq_metrics_cnt, \
            "Failed #4: The # of metric descriptors written from list_metrics  "\
            "doesn't match the # of records received from the Monitoring API call "\
            "bq_metrics_cnt: {}, metric_descriptors_cnt: {}"\
            .format(bq_metrics_cnt, metric_descriptors_cnt)

if __name__ == '__main__':
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    unittest.main()
