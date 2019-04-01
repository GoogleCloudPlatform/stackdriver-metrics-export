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
import argparse
import json
from googleapiclient.discovery import build
import config



# Compare the # from the metricDescriptors.list API and timeSeries.list
metric_results_api = {}
metric_descriptors = {}

# Compare the # from the timeSeries.list and what is available in BQ
timeseries_results_api = {}
timeseries_results_bq = {}

def build_timeseries_api_args(project_name="",
    batch_id="",
    filter_str="",
    end_time_str="",
    start_time_str="",
    aggregation_alignment_period="",
    group_by=config.GROUP_BY_STRING,
    perSeriesAligner=config.ALIGN_SUM,
    nextPageToken=""
    ):
    # build a dict with the API parameters
    api_args={}
    api_args["project_name"] = project_name
    api_args["metric_filter"] = filter_str
    api_args["end_time_str"] = end_time_str
    api_args["start_time_str"] = start_time_str
    api_args["aggregation_alignment_period"] = aggregation_alignment_period
    api_args["group_by"] = group_by
    api_args["perSeriesAligner"]=perSeriesAligner
    api_args["nextPageToken"]=nextPageToken

    return api_args

def get_timeseries_list(api_args):
    timeseries_resp_list = []
    while True:
        timeseries = get_timeseries(api_args)

        if timeseries:

            # retryable error codes based on https://developers.google.com/maps-booking/reference/grpc-api/status_codes
            if "executionErrors" in timeseries:
                if timeseries["executionErrors"]["code"] != 0:
                    print "Received an error getting the timeseries with code: {} and msg: {}".format(timeseries["executionErrors"]["code"],timeseries["executionErrors"]["message"])
                    logging.error("Received an error getting the timeseries with code: {} and msg: {}".format(timeseries["executionErrors"]["code"],timeseries["executionErrors"]["message"]))
                    break
            else:
                timeseries_resp_list.append(timeseries)
                if "nextPageToken" in timeseries:
                    api_args["nextPageToken"] = timeseries["nextPageToken"]
                else:
                    break

        else:
            logging.debug("No timeseries returned, no reason to write anything")
            print "No timeseries returned, no reason to write anything"
            msgs_without_timeseries=1
            break
    print "metric: {}".format(json.dumps(timeseries_resp_list,indent=4, sort_keys=True))
    return timeseries_resp_list

def get_bigquery_records(batch_id, metric_type):
    query ="SELECT " \
        "batch_id,metric.type,metric_kind,value_type,point.interval.start_time,point.interval.end_time,point.value.int64_value " \
        "FROM " \
        "`sage-facet-201016.metric_export.sd_metrics_export_fin` " \
        "WHERE " \
        "batch_id=\"{}\" AND metric.type=\"{}\"".format(batch_id,metric_type)


    response = query_bigquery(query)
    job_ref = response["jobReference"]
    results = get_query_results_bigquery(job_ref)
    return results

def query_bigquery(query):

    bigquery = build('bigquery', 'v2',cache_discovery=True)

    body = {
       "query": query,
       "useLegacySql": "false"
    }
    logging.debug('body: {}'.format(json.dumps(body,sort_keys=True, indent=4)))
    print 'body: {}'.format(json.dumps(body,sort_keys=True, indent=4))
    response = bigquery.jobs().query(
        projectId="sage-facet-201016",
        body=body
    ).execute()
    
    logging.debug("BigQuery said... = {}".format(response))
    print "BigQuery said... = {}".format(response)

    return response

def get_query_results_bigquery(job_ref):
    bigquery = build('bigquery', 'v2',cache_discovery=True)

    response = bigquery.jobs().getQueryResults(
        projectId="sage-facet-201016",
        jobId=job_ref["jobId"]
    ).execute()
    
    logging.debug("BigQuery said... = {}".format(response))
    print "BigQuery said... = {}".format(json.dumps(response,sort_keys=True, indent=4))

    return response

def get_metrics(next_page_token,filter_str=""):
    
    service = build('monitoring', 'v3',cache_discovery=True)
    project_name  = 'projects/{project_id}'.format(
        project_id="sage-facet-201016"
    )
    
    metrics = service.projects().metricDescriptors().list(
         name=project_name,
         pageToken=next_page_token,
         filter=filter_str
    ).execute()

    logging.debug("response is {}".format(json.dumps(metrics, sort_keys=True, indent=4)))
    return metrics

def check_exclusions(metric):
    exclusions = config.EXCLUSIONS
    for exclusion in exclusions['metricKinds']:
        logging.debug("exclusion check:  {},{}".format(metric['metricKind'],exclusion['metricKind']))
        if ((metric['metricKind'] == exclusion['metricKind']) and 
            (metric['valueType'] == exclusion['valueType'])):
            logging.debug("excluding based on metricKind {},{} AND {},{}".format(metric['metricKind'],exclusion['metricKind'],metric['valueType'],exclusion['valueType']))
            return False

    for exclusion in exclusions['metricTypes']:
        logging.debug("exclusion metricTypes check:  {},{}".format(metric['type'],exclusion['metricType']))
        if metric['type'].find(exclusion['metricType']) != -1:
            logging.debug("excluding based on metricType {},{}".format(metric['type'],exclusion['metricType']))
            return False        
            
    for exclusion in exclusions['metricTypeGroups']:
        logging.debug("exclusion metricTypeGroups check:  {},{}".format(metric['type'],exclusion['metricTypeGroup']))
        if metric['type'].find(exclusion['metricTypeGroup']) != -1:
            logging.debug("excluding based on metricTypeGroup {},{}".format(metric['type'],exclusion['metricTypeGroup']))
            return False        
    return True

def build_metric_descriptors_list():
    stats = {}
    msgs_published=0
    msgs_excluded=0
    metrics_count_from_api=0
    
    metrics_list ={}
    next_page_token=""
    while True:
        metric_list = get_metrics(next_page_token)

        metrics_count_from_api+=len(metric_list['metricDescriptors'])
        for metric in metric_list['metricDescriptors']:
            logging.debug("Processing metric {}".format(metric))
            if check_exclusions(metric):
                metric_type = metric["type"]
                metric_results_api[metric_type]=1
                metric_descriptors[metric_type]=metric
                msgs_published+=1
            else:
                logging.debug("Excluded the metric: {}".format(metric['name']))
                msgs_excluded+=1
        if "nextPageToken" in metric_list:
            next_page_token=metric_list["nextPageToken"]
        else:
            break
    stats["msgs_published"] = msgs_published
    stats["msgs_excluded"] = msgs_excluded
    stats["metrics_count_from_api"]=metrics_count_from_api
    return stats
    
def get_aligner_reducer(metric_kind, metric_val_type):
    if metric_kind==config.GAUGE:
        if metric_val_type==config.BOOL:
            crossSeriesReducer=config.REDUCE_MEAN
            perSeriesAligner=config.ALIGN_FRACTION_TRUE
        elif metric_val_type in [config.INT64,config.DOUBLE,config.DISTRIBUTION]:
            crossSeriesReducer=config.REDUCE_SUM
            perSeriesAligner=config.ALIGN_SUM
        elif metric_val_type==config.STRING:
            crossSeriesReducer=config.REDUCE_COUNT
            perSeriesAligner=config.ALIGN_NONE
        else:
            logging.debug("No match for GAUGE {},{}".format(metric_kind, metric_val_type))
    elif metric_kind==config.DELTA:
        if metric_val_type in [config.INT64,config.DOUBLE,config.DISTRIBUTION]:
            crossSeriesReducer=config.REDUCE_SUM
            perSeriesAligner=config.ALIGN_SUM
        else:
            logging.debug("No match for DELTA {},{}".format(metric_kind, metric_val_type))
    elif metric_kind==config.CUMULATIVE:
        if metric_val_type in [config.INT64, config.DOUBLE,config.DISTRIBUTION]:
            crossSeriesReducer=config.REDUCE_SUM
            perSeriesAligner=config.ALIGN_DELTA
        else:
            logging.debug("No match for CUMULATIVE {},{}".format(metric_kind, metric_val_type))
    else:
        logging.debug("No match for {},{}".format(metric_kind, metric_val_type))

    return crossSeriesReducer, perSeriesAligner


def get_and_count_timeseries(data):
    
    metric_type = data["metric"]['type']
    metric_kind = data["metric"]['metricKind']
    metric_val_type = data["metric"]['valueType']
    end_time_str = data["end_time"]
    start_time_str = data["start_time"]
    aggregation_alignment_period = data["aggregation_alignment_period"]

    logging.debug('get_timeseries for metric: {},{},{},{},{}'.format(metric_type,metric_kind, metric_val_type, start_time_str, end_time_str))
    
    project_name  = 'projects/{project_id}'.format(
        project_id="sage-facet-201016"
    )
    
    # Capture the stats 
    stats = {}
    msgs_published=0
    msgs_without_timeseries=0
    metrics_count_from_api=0

    # get the appropriate aligner based on the metric_kind and value_type
    crossSeriesReducer, perSeriesAligner = get_aligner_reducer(metric_kind,metric_val_type)

    # build a dict with the API parameters
    api_args={}
    api_args["project_name"] = project_name
    api_args["metric_filter"] = "metric.type=\"{}\" ".format(metric_type)
    api_args["end_time_str"] = data["end_time"]
    api_args["start_time_str"] = data["start_time"]
    api_args["aggregation_alignment_period"] = data["aggregation_alignment_period"]
    api_args["group_by"] = config.GROUP_BY_STRING
    api_args["crossSeriesReducer"]=crossSeriesReducer
    api_args["perSeriesAligner"]=perSeriesAligner
    api_args["nextPageToken"]=""

    # Call the projects.timeseries.list API
    timeseries ={}
    while True:
        timeseries = get_timeseries(api_args)

        if timeseries:

            # retryable error codes based on https://developers.google.com/maps-booking/reference/grpc-api/status_codes
            if "executionErrors" in timeseries:
                if timeseries["executionErrors"]["code"] != 0:
                    logging.error("Received an error getting the timeseries with code: {} and msg: {}".format(timeseries["executionErrors"]["code"],timeseries["executionErrors"]["message"]))
                    break
            else:
                # write the first timeseries
                if metric_type in timeseries_results_api: 
                    timeseries_results_api[metric_type] = len(timeseries["timeSeries"])
                else:
                    timeseries_results_api[metric_type] += len(timeseries["timeSeries"])
                metrics_count_from_api+=len(timeseries["timeSeries"])
                if "nextPageToken" in timeseries:
                    api_args["nextPageToken"] = timeseries["nextPageToken"]
                else:
                    break

        else:
            logging.debug("No timeseries returned, no reason to write anything")
            timeseries_results_api[metric_type]=0
            msgs_without_timeseries=1
            break

    stats["msgs_published"] = msgs_published
    stats["msgs_without_timeseries"] = msgs_without_timeseries
    stats["metrics_count_from_api"] = metrics_count_from_api
    logging.debug("Stats are {}".format(json.dumps(stats)))

    return response_code

def get_timeseries(api_args):
    service = build('monitoring', 'v3',cache_discovery=True)
    timeseries = service.projects().timeSeries().list(
        name=api_args["project_name"],
        filter=api_args["metric_filter"],
        aggregation_alignmentPeriod=api_args["aggregation_alignment_period"],
        #aggregation_crossSeriesReducer=api_args["crossSeriesReducer"],
        aggregation_perSeriesAligner=api_args["perSeriesAligner"],
        aggregation_groupByFields=api_args["group_by"],
        interval_endTime=api_args["end_time_str"],
        interval_startTime=api_args["start_time_str"],
        pageToken=api_args["nextPageToken"]
    ).execute()
    logging.debug('response: {}'.format(json.dumps(timeseries,sort_keys=True, indent=4)))
    return timeseries

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'batch_id',
        help='The batch_id to use in the test.')

    args = parser.parse_args()
    logging.debug("Running with batch_id: {}".format(args.batch_id))
    logging.basicConfig(filename='/home/cbaer/stackdriver-metrics-export/test/test.log',filemode='w',level=logging.DEBUG)
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    """
    stats = {}
    stats = build_metric_descriptors_list()
    print "stats: {}".format(json.dumps(stats, sort_keys=True, indent=4))

    for metric in metric_descriptors:
        get_and_count_timeseries(metric)
    """

    # test 1. the number of items in metric_descriptors should match the number of keys in timeseries_results_api

    # test 2. the count of each item in timeseries_results_api should match the count of each item in timeseries_results_bq

    """ Test the values match across metricDescriptors.list, timeseries.list and BigQuery table.
        You should pick a metric_type, start_time, end_time and batch_id from the Stackdriver Logs
    """    
    project_id="sage-facet-201016"
    project_name="projects/{}".format(project_id)
    batch_id = args.batch_id
    start_time_str = "2019-02-20T15:26:19.025680Z" 
    end_time_str = "2019-02-20T16:00:01.131179Z" 
    aggregation_alignment_period = "3600s"
    
    metric_type_list = [
        "monitoring.googleapis.com/stats/num_time_series",
        "pubsub.googleapis.com/subscription/push_request_count",
        "monitoring.googleapis.com/billing/bytes_ingested",
        "logging.googleapis.com/byte_count",
        "kubernetes.io/container/memory/request_utilization",
       # "compute.googleapis.com/instance/integrity/late_boot_validation_status",
        "serviceruntime.googleapis.com/api/request_count"
        ]

    for metric_type in metric_type_list:
        filter_str = 'metric.type = "{}"'.format(metric_type)
        api_args=build_timeseries_api_args(
            project_name,batch_id,filter_str,end_time_str,start_time_str,
            aggregation_alignment_period,config.GROUP_BY_STRING,config.ALIGN_SUM,"")

        timeseries_resp_list = get_timeseries_list(api_args)
        number_of_api_timeseries = 0
        for timeseries in timeseries_resp_list:
            
            for timeseries_rec in timeseries["timeSeries"]:
                point_cnt=len(timeseries_rec["points"])
                print "ALERT: {} point_cnt {}".format(metric_type,point_cnt)
                number_of_api_timeseries+= point_cnt 
                print "ALERT: {} - {}".format(metric_type,number_of_api_timeseries)
        
        results = get_bigquery_records(batch_id, metric_type)

        number_of_bq_timeseries = int(results["totalRows"])
        assert number_of_api_timeseries==number_of_bq_timeseries, "Count doesnt match for {} - API:{}, BQ:{}".format(
            metric_type,number_of_api_timeseries,number_of_bq_timeseries
        )



