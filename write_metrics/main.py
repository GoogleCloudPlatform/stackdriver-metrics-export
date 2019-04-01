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
import webapp2
import json
import base64
import config
from datetime import datetime 
from googleapiclient.discovery import build
from googleapiclient.discovery import HttpError
from google.appengine.api import app_identity


def build_rows(timeseries, metadata):
    """ Build a list of JSON object rows to insert into BigQuery
        This function may fan out the input by writing 1 entry into BigQuery for every point,
        if there is more than 1 point in the timeseries
    """
    logging.debug("build_row")
    rows = []

    # handle >= 1 points, potentially > 1 returned from Monitoring API call
    for point_idx in range(len(timeseries["points"])):
        row = {
            "batch_id": metadata["batch_id"]
        }

        metric = {
            "type": timeseries["metric"]["type"]
        }
        metric_labels_list = get_labels(timeseries["metric"], "labels")

        if len(metric_labels_list) > 0:
            metric["labels"] = metric_labels_list
        row["metric"] = metric

        resource = {
            "type": timeseries["resource"]["type"] 
        }
        resource_labels_list = get_labels(timeseries["resource"], "labels")
        if len(resource_labels_list) > 0:
            resource["labels"] = resource_labels_list

        row["resource"] = resource

        interval = {
            "start_time": timeseries["points"][point_idx]["interval"]["startTime"], 
            "end_time": timeseries["points"][point_idx]["interval"]["endTime"]
        }

        # map the API value types to the BigQuery value types
        value_type = timeseries["valueType"]
        bigquery_value_type_index = config.bigquery_value_map[value_type]
        api_value_type_index = config.api_value_map[value_type]
        value_type_label = {}
        value = timeseries["points"][point_idx]["value"][api_value_type_index]
        if value_type == config.DISTRIBUTION:
            value_type_label[bigquery_value_type_index] = build_distribution_value(value)
        else:
            value_type_label[bigquery_value_type_index] = value

        point = {
            "interval": interval,
            "value": value_type_label
        }
        row["point"] = point

        if "metadata" in timeseries:
            metric_metadata = {}
            if "userLabels" in timeseries["metadata"]:
                user_labels_list = get_labels(timeseries["metadata"], "userLabels")
                if len(user_labels_list) > 0:
                    metric_metadata["user_labels"] = user_labels_list

            if "systemLabels" in timeseries["metadata"]:
                system_labels_list = get_system_labels(timeseries["metadata"], "systemLabels")
                if len(system_labels_list) > 0:
                    metric_metadata["system_labels"] = system_labels_list

            row["metric_metadata"] = metric_metadata

        row["metric_kind"] = timeseries["metricKind"]
        row["value_type"] = timeseries["valueType"]
        row_to_insert = {}
        row_to_insert["json"] = row
        rows.append(row_to_insert)
        logging.debug('row: {}'.format(json.dumps(row,sort_keys=True, indent=4)))

    return rows


def get_labels(timeseries,label_name):
    """ Build a list of metric labels based on the API spec below
        See https://cloud.google.com/monitoring/api/ref_v3/rest/v3/TimeSeries#MonitoredResourceMetadata
    """
    metric_labels_list = []
    if label_name in timeseries:
        for label in timeseries[label_name]:
            metric_label = {}
            metric_label["key"] = label
            metric_label["value"] = timeseries[label_name][label]
            metric_labels_list.append(metric_label)
    logging.debug("get_labels: {}".format(json.dumps(metric_labels_list,sort_keys=True, indent=4)))
    return metric_labels_list


def get_system_labels(timeseries,label_name):
    """ Build a list of system_labels based on the API spec below
        See https://cloud.google.com/monitoring/api/ref_v3/rest/v3/TimeSeries#MonitoredResourceMetadata
    """
    system_labels_list = []
    if label_name in timeseries:
        for label in timeseries[label_name]:
            metric_label = {}
            metric_label["key"] = label
            # The values can be bool, list or str 
            # See https://cloud.google.com/monitoring/api/ref_v3/rest/v3/TimeSeries#MonitoredResourceMetadata
            if type(timeseries[label_name][label]) is bool:
                metric_label["value"] = str(timeseries[label_name][label])
                system_labels_list.append(metric_label)
            elif type(timeseries[label_name][label]) is list:
                for system_label_name in timeseries[label_name][label]:
                    metric_label = {}
                    metric_label["key"] = label
                    metric_label["value"] = system_label_name
                    system_labels_list.append(metric_label)
            else:    
                metric_label["value"] = timeseries[label_name][label]
                system_labels_list.append(metric_label)

    return system_labels_list


def build_distribution_value(value_json):
    """ Build a distribution value based on the API spec below
        See https://cloud.google.com/monitoring/api/ref_v3/rest/v3/TimeSeries#Distribution
    """
    distribution_value = {}
    if "count" in value_json:
        distribution_value["count"] = int(value_json["count"])
    if "mean" in value_json:
        distribution_value["mean"] = round(value_json["mean"],2)
    if "sumOfSquaredDeviation" in value_json:
        distribution_value["sumOfSquaredDeviation"] = round(value_json["sumOfSquaredDeviation"],2)

    if "range" in value_json:
        distribution_value_range = {}
        distribution_value_range["min"]=value_json["range"]["min"]
        distribution_value_range["max"]=value_json["range"]["max"]
        distribution_value["range"] = distribution_value_range

    bucketOptions = {}
    if "linearBuckets" in value_json["bucketOptions"]:
        linearBuckets = {
            "numFiniteBuckets": value_json["bucketOptions"]["linearBuckets"]["numFiniteBuckets"],
            "width": value_json["bucketOptions"]["linearBuckets"]["width"],
            "offset": value_json["bucketOptions"]["linearBuckets"]["offset"]
        }
        bucketOptions["linearBuckets"] = linearBuckets
    elif "exponentialBuckets" in value_json["bucketOptions"]:
        exponentialBuckets = {
            "numFiniteBuckets": value_json["bucketOptions"]["exponentialBuckets"]["numFiniteBuckets"],
            "growthFactor": round(value_json["bucketOptions"]["exponentialBuckets"]["growthFactor"],2),
            "scale": value_json["bucketOptions"]["exponentialBuckets"]["scale"]
        }
        bucketOptions["exponentialBuckets"] = exponentialBuckets
    elif "explicitBuckets" in value_json["bucketOptions"]:
        explicitBuckets = {
            "bounds": {
                "value": value_json["bucketOptions"]["explicitBuckets"]["bounds"]
            }

        }
        bucketOptions["explicitBuckets"] = explicitBuckets
    if bucketOptions:
        distribution_value["bucketOptions"] = bucketOptions

    if "bucketCounts" in value_json:
        bucketCounts = {}
        bucket_count_list = []
        for bucket_count_val in value_json["bucketCounts"]:
            bucket_count_list.append(int(bucket_count_val))
        bucketCounts["value"] = bucket_count_list
        distribution_value["bucketCounts"] = bucketCounts

    if "exemplars" in value_json:
        exemplars_list = []
        for exemplar in value_json["exemplars"]:
            exemplar = {
                "value": exemplar["value"],
                "timestamp": exemplar["timestamp"]
            }
            exemplars_list.append(exemplar)
        distribution_value["exemplars"] = exemplars_list

    logging.debug("created the distribution_value: {}".format(json.dumps(distribution_value,sort_keys=True, indent=4)))
    return distribution_value


def build_bigquery_stats_message(metadata):

    processing_end_time = datetime.now()
    processing_end_time_str = processing_end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # Write the stats to the BigQuery stats tabledata
    bq_msg = {
        "app_name": "write_metrics",
        "batch_id": metadata["batch_id"],
        "message_id": metadata["message_id"],
        "src_message_id": metadata["src_message_id"],
        "metric_type": metadata["metric_type"],
        "error_msg_cnt": metadata["error_msg_cnt"],
        "msg_written_cnt": metadata["msg_written_cnt"],
        "msg_without_timeseries": metadata["msg_without_timeseries"],
        "payload": metadata["payload"],
        "batch_start_time": metadata["batch_start_time"],
        "processing_end_time": processing_end_time_str
    }
    json_msg = {
        "json": bq_msg
    }
    logging.debug("json_msg {}".format(json.dumps(json_msg, sort_keys=True, indent=4)))
    return json_msg


class ReceiveMessage(webapp2.RequestHandler):
    """ Handle the Pub/Sub push messages
    """

    def write_stats_to_bigquery(self, json_row_list):
        """ Write rows to the BigQuery stats table using the googleapiclient and the streaming insertAll method
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
        """
        logging.debug("write_stats_to_bigquery")

        bigquery = build('bigquery', 'v2', cache_discovery=True)

        body = {
            "kind": "bigquery#tableDataInsertAllRequest",
            "skipInvalidRows": "false",
            "rows": json_row_list
        }
        logging.debug('body: {}'.format(json.dumps(body, sort_keys=True, indent=4)))

        response = bigquery.tabledata().insertAll(
            projectId=app_identity.get_application_id(),
            datasetId=config.BIGQUERY_DATASET,
            tableId=config.BIGQUERY_STATS_TABLE,
            body=body
        ).execute()
        logging.debug("BigQuery said... = {}".format(response))

        bq_stats_msgs_with_errors = 0
        if "insertErrors" in response:
            if len(response["insertErrors"]) > 0:
                logging.error("Error: {}".format(response))
                bq_stats_msgs_with_errors = len(response["insertErrors"])
        else:
            logging.debug("By amazing luck, there are no errors, response = {}".format(response))
        logging.debug("bq_stats_msgs_with_errors: {}".format(bq_stats_msgs_with_errors))
        return response

    def write_to_bigquery(self, timeseries, metadata):
        """ Write rows to BigQuery using the googleapiclient and the streaming insertAll method
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
        """
        logging.debug("write_to_bigquery")

        response = {}
        json_msg_list = []
        stats = {}
        if not timeseries or "points" not in timeseries:
            logging.debug("No timeseries data to write to BigQuery for: {}".format(timeseries))
            msgs_written = 0
            metadata["msg_without_timeseries"] = 1
            error_msg_cnt = 0
        else:

            rows = build_rows(timeseries, metadata)
            bigquery = build('bigquery', 'v2', cache_discovery=True)
            body = {
                "kind": "bigquery#tableDataInsertAllRequest",
                "skipInvalidRows": "false",
                "rows": rows
            }
            logging.debug('body: {}'.format(json.dumps(body, sort_keys=True, indent=4)))

            response = bigquery.tabledata().insertAll(
                projectId=app_identity.get_application_id(),
                datasetId=config.BIGQUERY_DATASET,
                tableId=config.BIGQUERY_TABLE,
                body=body
            ).execute()

            logging.debug("BigQuery said... = {}".format(response))

            msgs_written = len(rows)
            error_msg_cnt = 0

            if "insertErrors" in response:
                if len(response["insertErrors"]) > 0:
                    logging.error("Error: {}".format(response))
                    error_msg_cnt = len(response["insertErrors"])
                    msgs_written = msgs_written - error_msg_cnt
            else:
                logging.debug("By amazing luck, there are no errors, response = {}".format(response))

            metadata["msg_without_timeseries"] = 0

        # set the metadata to write to the BigQuery stats table
        if config.WRITE_BQ_STATS_FLAG:
            metadata["error_msg_cnt"] = error_msg_cnt
            metadata["msg_written_cnt"] = msgs_written
            metadata["payload"] = '{}'.format(json.dumps(timeseries))
            metadata["metric_type"] = timeseries["metric"]["type"]
            json_msg = build_bigquery_stats_message(metadata)
            json_msg_list.append(json_msg)

            # write the list of stats messages to BigQuery
            self.write_stats_to_bigquery(json_msg_list)

        stats["msgs_written"] = msgs_written
        stats["msgs_with_errors"] = error_msg_cnt
        logging.info("Stats are {}".format(json.dumps(stats)))
        return response

    def post(self):
        """ Receive the Pub/Sub message via POST
            Validate the input and then process the message
        """
        logging.debug("received message")

        response_code = 200
        try:
            if not self.request.body:
                raise ValueError("No request body received")
            envelope = json.loads(self.request.body.decode('utf-8'))
            logging.debug("Raw pub/sub message: {}".format(envelope))

            if "message" not in envelope:
                raise ValueError("No message in envelope")

            if "messageId" in envelope["message"]:
                logging.debug("messageId: {}".format(envelope["message"]["messageId"]))
            message_id = envelope["message"]["messageId"]

            if "attributes" not in envelope["message"]:
                raise ValueError("Attributes such as token and batch_id missing from request")

            # if the pubsub PUBSUB_VERIFICATION_TOKEN isn't included or doesn't match, don't continue
            if "token" not in envelope['message']['attributes']:
                raise ValueError("token missing from request")
            if not envelope["message"]["attributes"]["token"] == config.PUBSUB_VERIFICATION_TOKEN:
                raise ValueError(
                    "token from request doesn't match, received: {}"
                    .format(envelope["message"]["attributes"]["token"])
                )

            # if the batch_id isn't included, fail immediately
            if "batch_id" not in envelope['message']['attributes']:
                raise ValueError("batch_id missing from request")
            batch_id = envelope["message"]["attributes"]["batch_id"]
            logging.debug("batch_id: {} ".format(batch_id))

            if "batch_start_time" in envelope["message"]["attributes"]:
                batch_start_time = envelope["message"]["attributes"]["batch_start_time"]
            else:
                batch_start_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

            if "src_message_id" in envelope["message"]["attributes"]:
                src_message_id = envelope["message"]["attributes"]["src_message_id"]
            else:
                src_message_id = ""

            if "data" not in envelope["message"]:
                raise ValueError("No data in message")
            payload = base64.b64decode(envelope["message"]["data"])
            logging.debug('payload: {} '.format(payload))

            metadata = {
                "batch_id": batch_id,
                "message_id": message_id,
                "src_message_id": src_message_id,
                "batch_start_time": batch_start_time
            }

            data = json.loads(payload)
            logging.debug('data: {} '.format(data))

            # Check the input parameters
            if not data:
                raise ValueError("No data in Pub/Sub Message to write to BigQuery")

            # See https://cloud.google.com/monitoring/api/ref_v3/rest/v3/TimeSeries 
            self.write_to_bigquery(data, metadata)

        except KeyError as ke:
            logging.error("Encountered KeyError: {}".format(ke))
            self.response.write(ke)
        except HttpError as he:
            logging.error("Encountered exception calling APIs: {}".format(he))
            self.response.write(he)
        except ValueError as ve:
            logging.error("Value Error: {}".format(ve))
            self.response.write(ve)

        self.response.status = response_code


app = webapp2.WSGIApplication([
    ('/_ah/push-handlers/receive_message', ReceiveMessage)
], debug=True)
