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


def get_aligner_reducer(metric_kind, metric_val_type):
    """ Returns the appropriate erSeriesAligner and crossSeriesReducer given the inputs
    """
    if metric_kind == config.GAUGE:
        if metric_val_type == config.BOOL:
            crossSeriesReducer = config.REDUCE_MEAN
            perSeriesAligner = config.ALIGN_FRACTION_TRUE
        elif metric_val_type in [config.INT64, config.DOUBLE, config.DISTRIBUTION]:
            crossSeriesReducer = config.REDUCE_SUM
            perSeriesAligner = config.ALIGN_SUM
        elif metric_val_type == config.STRING:
            crossSeriesReducer = config.REDUCE_COUNT
            perSeriesAligner = config.ALIGN_NONE
        else:
            logging.debug("No match for GAUGE {},{}".format(metric_kind, metric_val_type))
    elif metric_kind == config.DELTA:
        if metric_val_type in [config.INT64, config.DOUBLE, config.DISTRIBUTION]:
            crossSeriesReducer = config.REDUCE_SUM
            perSeriesAligner = config.ALIGN_SUM
        else:
            logging.debug("No match for DELTA {},{}".format(metric_kind, metric_val_type))
    elif metric_kind == config.CUMULATIVE:
        if metric_val_type in [config.INT64, config.DOUBLE, config.DISTRIBUTION]:
            crossSeriesReducer = config.REDUCE_SUM
            perSeriesAligner = config.ALIGN_DELTA
        else:
            logging.debug("No match for CUMULATIVE {},{}".format(metric_kind, metric_val_type))
    else:
        logging.debug("No match for {},{}".format(metric_kind, metric_val_type))

    return crossSeriesReducer, perSeriesAligner


class ReceiveMessage(webapp2.RequestHandler):
    """ Handle the Pub/Sub push messages
    """

    def get_and_publish_timeseries(self, data, metadata):
        """ Given a metricDescriptor object described below and a batch_id, as input
            https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.metricDescriptors#MetricDescriptor

            Get the timeseries for the metric and then publish each individual timeseries described below
            https://cloud.google.com/monitoring/api/ref_v3/rest/v3/TimeSeries       
            as a separate Pub/Sub message. This means there is a fan-out from 1 metric to 1 or more 
            timeseries objects.
        """
        metric_type = data["metric"]['type']
        metric_kind = data["metric"]['metricKind']
        metric_val_type = data["metric"]['valueType']
        end_time_str = data["end_time"]
        start_time_str = data["start_time"]
        project_id = data["project_id"]

        logging.debug('get_timeseries for metric: {},{},{},{},{}'.format(
            metric_type, metric_kind, metric_val_type, start_time_str, end_time_str)
        )
        project_name = 'projects/{project_id}'.format(
            project_id=project_id
        )
        # Capture the stats 
        stats = {}
        msgs_published = 0
        msgs_without_timeseries = 0
        metrics_count_from_api = 0

        # get the appropriate aligner based on the metric_kind and value_type
        crossSeriesReducer, perSeriesAligner = get_aligner_reducer(
            metric_kind, metric_val_type
        )

        # build a dict with the API parameters
        api_args = {}
        api_args["project_name"] = project_name
        api_args["metric_filter"] = "metric.type=\"{}\" ".format(metric_type)
        api_args["end_time_str"] = data["end_time"]
        api_args["start_time_str"] = data["start_time"]
        api_args["aggregation_alignment_period"] = data["aggregation_alignment_period"]
        api_args["group_by"] = config.GROUP_BY_STRING
        api_args["crossSeriesReducer"] = crossSeriesReducer
        api_args["perSeriesAligner"] = perSeriesAligner
        api_args["nextPageToken"] = ""

        # Call the projects.timeseries.list API
        response_code = 200
        timeseries = {}
        while True:
            try:
                timeseries = self.get_timeseries(api_args)
            except HttpError as he:
                metadata["error_msg_cnt"] = 1
                logging.error(
                    "Exception calling Monitoring API: {}".format(he)
                )

            if timeseries:

                # retryable error codes based on https://developers.google.com/maps-booking/reference/grpc-api/status_codes
                if "executionErrors" in timeseries:
                    if timeseries["executionErrors"]["code"] != 0:
                        response_code = 500
                        logging.error(
                            "Received an error getting the timeseries with code: {} and msg: {}".format(
                                timeseries["executionErrors"]["code"],
                                timeseries["executionErrors"]["message"])
                        )
                        break
                else:
                    # write the timeseries
                    msgs_published += self.publish_timeseries(timeseries, metadata)
                    metrics_count_from_api += len(timeseries["timeSeries"])
                    if "nextPageToken" in timeseries:
                        api_args["nextPageToken"] = timeseries["nextPageToken"]
                    else:
                        break

            else:
                logging.debug("No timeseries returned, no publish to pubsub")

                # build a list of stats message to write to BigQuery
                metadata["msg_written_cnt"] = 0
                metadata["msg_without_timeseries"] = 1
                if "error_msg_cnt" not in metadata:
                    metadata["error_msg_cnt"] = 0
                metadata["payload"] = ""
                metadata["metric_type"] = metric_type
                json_msg = self.build_bigquery_stats_message(metadata)
                json_msg_list = []
                json_msg_list.append(json_msg)

                msgs_published += 1
                # write the list of stats messages to BigQuery
                self.write_to_bigquery(json_msg_list)
                msgs_without_timeseries = 1
                break

        stats["msgs_published"] = msgs_published
        stats["msgs_without_timeseries"] = msgs_without_timeseries
        stats["metrics_count_from_api"] = metrics_count_from_api
        logging.debug("Stats are {}".format(json.dumps(stats)))

        return response_code

    def get_timeseries(self, api_args):
        """ Call the https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.timeSeries/list
            using the googleapiclient 
        """
        service = build('monitoring', 'v3', cache_discovery=True)
        timeseries = service.projects().timeSeries().list(
            name=api_args["project_name"],
            filter=api_args["metric_filter"],
            aggregation_alignmentPeriod=api_args["aggregation_alignment_period"],
            # aggregation_crossSeriesReducer=api_args["crossSeriesReducer"],
            aggregation_perSeriesAligner=api_args["perSeriesAligner"],
            aggregation_groupByFields=api_args["group_by"],
            interval_endTime=api_args["end_time_str"],
            interval_startTime=api_args["start_time_str"],
            pageSize=config.PAGE_SIZE,
            pageToken=api_args["nextPageToken"]
        ).execute()
        logging.debug('response: {}'.format(json.dumps(timeseries, sort_keys=True, indent=4)))
        return timeseries

    def get_pubsub_message(self, one_timeseries, metadata):
        logging.debug("pubsub msg is {}".format(json.dumps(one_timeseries, sort_keys=True, indent=4)))
        data = json.dumps(one_timeseries).encode('utf-8')
        message = {

            "data": base64.b64encode(data),
            "attributes": {
                "batch_id": metadata["batch_id"],
                "token": config.PUBSUB_VERIFICATION_TOKEN,
                "src_message_id": metadata["message_id"],
                "batch_start_time": metadata["batch_start_time"]
            }
        }

        logging.debug("pubsub msg is {}".format(json.dumps(message, sort_keys=True, indent=4)))
        return message

    def publish_metrics(self, msg_list):
        """ Call the https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/publish
            using the googleapiclient to publish a message to Pub/Sub.
            The token and batch_id are included as attributes
        """
        service = build('pubsub', 'v1', cache_discovery=True)
        topic_path = 'projects/{project_id}/topics/{topic}'.format(
            project_id=app_identity.get_application_id(),
            topic=config.PUBSUB_TOPIC
        )
        body = {
            "messages": msg_list
        }
        logging.debug("pubsub msg is {}".format(json.dumps(body, sort_keys=True, indent=4)))
        response = service.projects().topics().publish(
            topic=topic_path, body=body
        ).execute()
        logging.debug("response is {}".format(json.dumps(response, sort_keys=True, indent=4)))

    def publish_timeseries(self, request, metadata):
        """ Call the https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/publish
            using the googleapiclient to publish a message to Pub/Sub
        """

        msgs_published = 0
        json_msg_list = []
        pubsub_msg_list = []
        # handle >= 1 timeSeries, potentially > 1 returned from Monitoring API call
        for one_timeseries in request["timeSeries"]:

            message = self.get_pubsub_message(one_timeseries, metadata)
            pubsub_msg_list.append(message)

            # build a list of stats messages to write to BigQuery
            metadata["msg_written_cnt"] = 1
            metadata["msg_without_timeseries"] = 0
            metadata["error_msg_cnt"] = 0
            metadata["payload"] = '{}'.format(json.dumps(one_timeseries))
            metadata["metric_type"] = one_timeseries["metric"]["type"]
            if config.WRITE_BQ_STATS_FLAG:
                json_msg = self.build_bigquery_stats_message(metadata)
                json_msg_list.append(json_msg)

            msgs_published += 1

        # Write the messages to pubsub
        self.publish_metrics(pubsub_msg_list)

        # write the list of stats messages to BigQuery
        if config.WRITE_BQ_STATS_FLAG:
            self.write_to_bigquery(json_msg_list)

        return msgs_published

    def build_bigquery_stats_message(self, metadata):

        processing_end_time = datetime.now()
        processing_end_time_str = processing_end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        # Write the stats to the BigQuery stats tabledata
        bq_msg = {
            "app_name": "get_timeseries",
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

    def write_to_bigquery(self, json_row_list):
        """ Write rows to the BigQuery stats table using the googleapiclient and the streaming insertAll method
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
        """
        logging.debug("write_to_bigquery")

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

        bq_msgs_with_errors = 0
        if "insertErrors" in response:
            if len(response["insertErrors"]) > 0:
                logging.error("Error: {}".format(response))
                bq_msgs_with_errors = len(response["insertErrors"])
                logging.debug("bq_msgs_with_errors: {}".format(bq_msgs_with_errors))
        else:
            logging.debug("By amazing luck, there are no errors, response = {}".format(response))
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

            if "data" not in envelope["message"]:
                raise ValueError("No data in message")
            payload = base64.b64decode(envelope["message"]["data"])
            logging.debug('payload: {} '.format(payload))

            # if the pubsub PUBSUB_VERIFICATION_TOKEN isn't included or doesn't match, don't continue
            if "token" not in envelope["message"]["attributes"]:
                raise ValueError("token missing from request")
            if not envelope["message"]["attributes"]["token"] == config.PUBSUB_VERIFICATION_TOKEN:
                raise ValueError("token from request doesn't match")

            # if the batch_id isn't included, fail immediately
            if "batch_id" not in envelope["message"]["attributes"]:
                raise ValueError("batch_id missing from request")
            batch_id = envelope["message"]["attributes"]["batch_id"]
            logging.debug("batch_id: {}".format(batch_id))

            if "batch_start_time" in envelope["message"]["attributes"]:
                batch_start_time = envelope["message"]["attributes"]["batch_start_time"]
            else:
                batch_start_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

            if "src_message_id" in envelope["message"]["attributes"]:
                src_message_id = envelope["message"]["attributes"]["src_message_id"]

            data = json.loads(payload)
            logging.debug('data: {} '.format(data))

            # Check the input parameters
            if not data:
                raise ValueError("No data in Pub/Sub Message")
            if "metric" not in data:
                raise ValueError("Missing metric key in Pub/Sub message")
            if "type" not in data["metric"]:
                raise ValueError("Missing metric[type] key in Pub/Sub message")
            if "metricKind" not in data["metric"]:
                raise ValueError("Missing metric[metricKind] key in Pub/Sub message")
            if "valueType" not in data["metric"]:
                raise ValueError("Missing metric[valueType] key in Pub/Sub message")
            if "end_time" not in data:
                raise ValueError("Missing end_time key in Pub/Sub message")
            if "start_time" not in data:
                raise ValueError("Missing start_time key in Pub/Sub message")
            if "aggregation_alignment_period" not in data:
                raise ValueError("Missing aggregation_alignment_period key in Pub/Sub message")
            if "project_id" not in data:
                data["project_id"] = app_identity.get_application_id()   


            metadata = {
                "batch_id": batch_id,
                "message_id": message_id,
                "src_message_id": src_message_id,
                "batch_start_time": batch_start_time
            }
            # get the metrics and publish to Pub/Sub
            response_code = self.get_and_publish_timeseries(data, metadata)

        except ValueError as ve:
            logging.error("Missing inputs from Pub/Sub: {}".format(ve))
            self.response.write(ve)
        except KeyError as ke:
            logging.error("Key Error: {}".format(ke))
            self.response.write(ke)
        except HttpError as he:
            logging.error("Encountered exception calling APIs: {}".format(he))
            self.response.write(he)

        self.response.status = response_code


app = webapp2.WSGIApplication([
    ('/_ah/push-handlers/receive_message', ReceiveMessage)
], debug=True)
