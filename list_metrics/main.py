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
from datetime import datetime
from datetime import timedelta
from googleapiclient.discovery import build
from google.appengine.api import app_identity
import os
import cloudstorage as gcs
from cloudstorage import NotFoundError
import config
import random
import string
import re


def set_last_end_time(project_id, bucket_name, end_time_str, offset):
    """ Write the end_time as a string value in a JSON object in GCS. 
        This file is used to remember the last end_time in case one isn't provided
    """
    # get the datetime object
    end_time = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    delta = timedelta(seconds=offset)
    # Add offset seconds & convert back to str
    end_time_calc = end_time + delta
    end_time_calc_str = end_time_calc.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    file_name = '{}.{}'.format(project_id, config.LAST_END_TIME_FILENAME)

    logging.debug("set_last_end_time - end_time_str: {}, end_time_Calc_str: {}".format(
            end_time_str, end_time_calc_str)
    )
    end_time_str_json = {
        "end_time": end_time_calc_str
    }
    write_retry_params = gcs.RetryParams(backoff_factor=1.1)
    gcs_file = gcs.open('/{}/{}'.format(
        bucket_name, file_name),
                        'w',
                        content_type='text/plain',
                        retry_params=write_retry_params)
    gcs_file.write(json.dumps(end_time_str_json))
    gcs_file.close()

    return end_time_calc_str

def get_last_end_time(project_id, bucket_name):
    """ Get the end_time as a string value from a JSON object in GCS. 
        This file is used to remember the last end_time in case one isn't provided
    """
    last_end_time_str = ""
    file_name = '{}.{}'.format(project_id, config.LAST_END_TIME_FILENAME)
    logging.debug("get_last_end_time - file_name: {}".format(file_name))
    try:
        gcs_file = gcs.open('/{}/{}'.format(
            bucket_name, file_name))
        contents = gcs_file.read()
        logging.debug("GCS FILE CONTENTS: {}".format(contents))
        json_contents = json.loads(contents) 
        last_end_time_str = json_contents["end_time"]
        gcs_file.close()
    except NotFoundError as nfe:
        logging.error("Missing file when reading {} from GCS: {}".format(file_name, nfe))
        last_end_time_str = None
    except Exception as e:
        logging.error("Received error when reading {} from GCS: {}".format(file_name,e))
        last_end_time_str = None

    return last_end_time_str


def publish_metrics(msg_list):
    """ Call the https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/publish
        using the googleapiclient to publish a message to Pub/Sub.
        The token and batch_id are included as attributes
    """
    if len(msg_list) > 0:
        service = build('pubsub', 'v1', cache_discovery=True)
        topic_path = 'projects/{project_id}/topics/{topic}'.format(
            project_id=app_identity.get_application_id(),
            topic=config.PUBSUB_TOPIC
        )
        body = {
            "messages": msg_list
        }
        #logging.debug("pubsub msg is {}".format(json.dumps(body, sort_keys=True, indent=4)))
        response = service.projects().topics().publish(
            topic=topic_path, body=body
        ).execute()
        #logging.debug("response is {}".format(json.dumps(response, sort_keys=True, indent=4)))
    else:
        logging.debug("No pubsub messages to publish")


def get_message_for_publish_metric(request, metadata):
    """ Build a message for the https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/publish
        using the googleapiclient to publish a message to Pub/Sub.
        The token and batch_id are included as attributes
    """
    #logging.debug("sending message is {}".format(json.dumps(request, sort_keys=True, indent=4)))

    data = json.dumps(request).encode('utf-8')

    message = {
        "data": base64.b64encode(data),
        "attributes": {
            "batch_id": metadata["batch_id"],
            "token": config.PUBSUB_VERIFICATION_TOKEN,
            "batch_start_time": metadata["batch_start_time"],
            "src_message_id": metadata["message_id"]
        }
    }
    #logging.debug("pubsub message is {}".format(json.dumps(message, sort_keys=True, indent=4)))
    return message


def get_batch_id():
    """ Generate a unique id to use across the batches to uniquely identify each one
    """
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(32))


def check_date_format(date_str):
    """ Check the date to ensure that it's in the proper format
    """
    pattern = re.compile("^\d{4}-+\d{2}-+\d{2}T+\d{2}:+\d{2}:+\d{2}.+\d{1,}Z+$")
    matched = pattern.match(date_str)
    return matched


def check_exclusions(metric):
    """ Check whether to exclude a metric based on the inclusions OR exclusions list. 
        Note that this checks inclusions first.
        returns True for metrics to include
        returns False for metrics to exclude
    """
    inclusions = config.INCLUSIONS
    if "include_all" in inclusions and inclusions["include_all"] == config.ALL:
        #logging.debug("including based on include_all setting {},{}".format(metric['type'],inclusions["include_all"]))
        return True

    if 'metricKinds' in inclusions:
        for inclusion in inclusions['metricKinds']:
            #logging.debug("inclusion check:  {},{}".format(metric['metricKind'],inclusion['metricKind']))
            if ((metric['metricKind'] == inclusion['metricKind']) and
                (metric['valueType'] == inclusion['valueType'])):
                #logging.debug("including based on metricKind {},{} AND {},{}".format(metric['metricKind'],inclusion['metricKind'],metric['valueType'],inclusion['valueType']))
                return True

    if 'metricTypes' in inclusions:
        for inclusion in inclusions['metricTypes']:
            #logging.debug("inclusion metricTypes check:  {},{}".format(metric['type'],inclusion['metricType']))
            if metric['type'].find(inclusion['metricType']) != -1:
                #logging.debug("including based on metricType {},{}".format(metric['type'],inclusion['metricType']))
                return True

    if 'metricTypeGroups' in inclusions:
        for inclusion in inclusions['metricTypeGroups']:
            #logging.debug("inclusion metricTypes check:  {},{}".format(metric['type'],inclusion['metricTypeGroup']))
            if metric['type'].find(inclusion['metricTypeGroup']) != -1:
                logging.debug("including based on metricTypeGroups {},{}".format(metric['type'],inclusion['metricTypeGroup']))
                return True

    exclusions = config.EXCLUSIONS
    if "exclude_all" in exclusions and exclusions["exclude_all"] == config.ALL:
        #logging.debug("excluding based on exclude_all setting {},{}".format(metric['type'],exclusions["exclude_all"]))
        return False

    if 'metricKinds' in exclusions:
        for exclusion in exclusions['metricKinds']:
            #logging.debug("exclusion check:  {},{}".format(metric['metricKind'],exclusion['metricKind']))
            if ((metric['metricKind'] == exclusion['metricKind']) and
                (metric['valueType'] == exclusion['valueType'])):
                #logging.debug("excluding based on metricKind {},{} AND {},{}".format(metric['metricKind'],exclusion['metricKind'],metric['valueType'],exclusion['valueType']))
                return False

    if 'metricTypes' in exclusions:
        for exclusion in exclusions['metricTypes']:
            #logging.debug("exclusion metricTypes check:  {},{}".format(metric['type'],exclusion['metricType']))
            if metric['type'].find(exclusion['metricType']) != -1:
                #logging.debug("excluding based on metricType {},{}".format(metric['type'],exclusion['metricType']))
                return False

    if 'metricTypeGroups' in exclusions:
        for exclusion in exclusions['metricTypeGroups']:
            #logging.debug("exclusion metricTypeGroups check:  {},{}".format(metric['type'],exclusion['metricTypeGroup']))
            if metric['type'].find(exclusion['metricTypeGroup']) != -1:
                #logging.debug("excluding based on metricTypeGroup {},{}".format(metric['type'],exclusion['metricTypeGroup']))
                return False
    return True


def get_metrics(project_id, next_page_token):
    """ Call the https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.metricDescriptors/list
        using the googleapiclient to get all the metricDescriptors for the project
    """

    service = build('monitoring', 'v3', cache_discovery=True)
    project_name = 'projects/{project_id}'.format(
        project_id=project_id
    )

    metrics = service.projects().metricDescriptors().list(
         name=project_name,
         pageSize=config.PAGE_SIZE,
         pageToken=next_page_token
    ).execute()

    logging.debug("project_id: {}, size: {}".format(
        project_id,
        len(metrics["metricDescriptors"])
        )
    )
    return metrics


def get_and_publish_metrics(message_to_publish, metadata):
    """ Publish the direct JSON results of each metricDescriptor as a separate Pub/Sub message
    """

    stats = {}
    msgs_published = 0
    msgs_excluded = 0
    metrics_count_from_api = 0

    next_page_token = ""
    while True:
        json_msg_list = []
        pubsub_msg_list = []

        project_id = message_to_publish["project_id"]
        metric_list = get_metrics(project_id, next_page_token)

        metrics_count_from_api += len(metric_list['metricDescriptors'])
        for metric in metric_list['metricDescriptors']:
            #logging.debug("Processing metric {} for publish".format(metric))
            metadata["payload"] = '{}'.format(json.dumps(metric))
            metadata["error_msg_cnt"] = 0

            message_to_publish["metric"] = metric
            if check_exclusions(metric):
                pubsub_msg = get_message_for_publish_metric(
                    message_to_publish, metadata
                )
                pubsub_msg_list.append(pubsub_msg)
                metadata["msg_written_cnt"] = 1
                metadata["msg_without_timeseries"] = 0
                msgs_published += 1
            else:
                #logging.debug("Excluded the metric: {}".format(metric['name']))
                msgs_excluded += 1
                metadata["msg_written_cnt"] = 0
                metadata["msg_without_timeseries"] = 1

            # build a list of stats messages to write to BigQuery
            if config.WRITE_BQ_STATS_FLAG:
                json_msg = build_bigquery_stats_message(
                    message_to_publish, metadata
                )
                json_msg_list.append(json_msg)

        # Write to pubsub if there is 1 or more 
        publish_metrics(pubsub_msg_list)

        # write the list of stats messages to BigQuery
        if config.WRITE_BQ_STATS_FLAG:
            write_to_bigquery(json_msg_list)

        if "nextPageToken" in metric_list:
            next_page_token = metric_list["nextPageToken"]
        else:
            break
    stats["msgs_published"] = msgs_published
    stats["msgs_excluded"] = msgs_excluded
    stats["metrics_count_from_api"] = metrics_count_from_api

    return stats


def write_stats(stats, stats_project_id, batch_id):
    """ Write 3 custom monitoring metrics to the Monitoring API
    """
    logging.debug("write_stats: {}".format(json.dumps(stats)))
    service = build('monitoring', 'v3',cache_discovery=True)
    project_name = 'projects/{project_id}'.format(
        project_id=app_identity.get_application_id()
    )

    end_time = datetime.now()
    end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    metric_type = "custom.googleapis.com/stackdriver-monitoring-export/msgs-published"
    body ={
        "timeSeries": [
            {
                "metric": {
                    "type": metric_type,
                    "labels": {
                        "batch_id": batch_id,
                        "metrics_project_id": stats_project_id
                    }
                },
                "resource": {
                    "type": "generic_node",
                    "labels": {
                        "project_id": app_identity.get_application_id(),
                        "location": "us-central1-a",
                        "namespace": "stackdriver-metric-export",
                        "node_id": "list-metrics"
                    }
                },
                "metricKind": "GAUGE",
                "valueType": "INT64", 
                "points": [
                    {
                        "interval": {
                            "endTime": end_time_str
                        },
                        "value": {
                            "int64Value": stats["msgs_published"]
                        }
                    }
                ]

            }
        ]
    }

    metrics = service.projects().timeSeries().create(
        name=project_name,
        body=body
    ).execute()
    logging.debug("wrote a response is {}".format(json.dumps(metrics, sort_keys=True, indent=4)))

    body["timeSeries"][0]["metric"]["type"] = "custom.googleapis.com/stackdriver-monitoring-export/msgs-excluded"
    body["timeSeries"][0]["points"][0]["value"]["int64Value"] = stats["msgs_excluded"]
    metrics = service.projects().timeSeries().create(
        name=project_name,
        body=body
    ).execute()
    logging.debug("response is {}".format(json.dumps(metrics, sort_keys=True, indent=4)))

    body["timeSeries"][0]["metric"]["type"] = "custom.googleapis.com/stackdriver-monitoring-export/metrics-from-api"
    body["timeSeries"][0]["points"][0]["value"]["int64Value"] = stats["metrics_count_from_api"]
    metrics = service.projects().timeSeries().create(
        name=project_name,
        body=body
    ).execute()
    logging.debug("response is {}".format(json.dumps(metrics, sort_keys=True, indent=4)))


def build_bigquery_stats_message(metric, metadata):

    processing_end_time = datetime.now()
    processing_end_time_str = processing_end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # Write the stats to the BigQuery stats tabledata
    bq_msg = {
        "app_name": "list_metrics",
        "batch_id": metadata["batch_id"],
        "message_id": metadata["message_id"],
        # "src_message_id": src_message_id,
        "metric_type": metric["metric"]["type"],
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
    #logging.debug("json_msg {}".format(json.dumps(json_msg, sort_keys=True, indent=4)))
    return json_msg


def write_to_bigquery(json_row_list):
    """ Write rows to the BigQuery stats table using the googleapiclient and the streaming insertAll method
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
    """
    #logging.debug("write_to_bigquery")

    if len(json_row_list) > 0:
        bigquery = build('bigquery', 'v2', cache_discovery=True)

        body = {
            "kind": "bigquery#tableDataInsertAllRequest",
            "skipInvalidRows": "false",
            "rows": json_row_list
        }
        #logging.debug('body: {}'.format(json.dumps(body, sort_keys=True, indent=4)))

        response = bigquery.tabledata().insertAll(
            projectId=app_identity.get_application_id(),
            datasetId=config.BIGQUERY_DATASET,
            tableId=config.BIGQUERY_STATS_TABLE,
            body=body
        ).execute()
        #logging.debug("BigQuery said... = {}".format(response))

        bq_msgs_with_errors = 0
        if "insertErrors" in response:
            if len(response["insertErrors"]) > 0:
                logging.error("Error: {}".format(response))
                bq_msgs_with_errors = len(response["insertErrors"])
        else:
            logging.debug("By amazing luck, there are no errors, response = {}".format(response))
        logging.debug("bq_msgs_written: {}".format(bq_msgs_with_errors))
        return response
    else:
        logging.debug("No BigQuery records to write")
        return None


def write_input_parameters_to_bigquery(project_id, metadata, msg):
    """ Write rows to the BigQuery input parameters table using the 
        googleapiclient and the streaming insertAll method
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
    """
    #logging.debug("write_input_parameters_to_bigquery")

    bigquery = build('bigquery', 'v2', cache_discovery=True)

    body = {
        "kind": "bigquery#tableDataInsertAllRequest",
        "skipInvalidRows": "false",
        "rows": [
            {
                "json":
                {
                    "start_time": msg["start_time"],
                    "end_time": msg["end_time"],
                    "aggregation_alignment_period": msg["aggregation_alignment_period"],
                    "message_id": metadata["message_id"],
                    "project_list": {
                        "project_id": [
                            project_id
                        ]
                    },
                    "batch_id": metadata["batch_id"],
                    "batch_start_time": metadata["batch_start_time"]
                }
            }
        ]
    }
    #logging.debug('body: {}'.format(json.dumps(body, sort_keys=True, indent=4)))

    response = bigquery.tabledata().insertAll(
        projectId=app_identity.get_application_id(),
        datasetId=config.BIGQUERY_DATASET,
        tableId=config.BIGQUERY_PARAMS_TABLE,
        body=body
    ).execute()
    #logging.debug("BigQuery said... = {}".format(response))

    bq_msgs_with_errors = 0
    if "insertErrors" in response:
        if len(response["insertErrors"]) > 0:
            logging.error("Error: {}".format(response))
            bq_msgs_with_errors = len(response["insertErrors"])
    else:
        logging.debug("By amazing luck, there are no errors, response = {}".format(response))
    logging.debug("bq_msgs_written: {}".format(bq_msgs_with_errors))
    return response


class ReceiveMessage(webapp2.RequestHandler):
    """ Handle the Pub/Sub push messages
    """

    def post(self):
        """ Receive the Pub/Sub message via POST
            Validate the input and then process the message
        """
        logging.debug("received message")

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

            if "publishTime" in envelope["message"]:
                publish_time = envelope["message"]["publishTime"]

            if "data" not in envelope["message"]:
                raise ValueError("No data in message")
            payload = base64.b64decode(envelope["message"]["data"])

            logging.debug('payload: {} '.format(payload))
            data = json.loads(payload)
            logging.debug('data: {} '.format(data))

            # Add any of the parameters to the pubsub message to send
            message_to_publish = {}

            # if the pubsub PUBSUB_VERIFICATION_TOKEN isn't included or doesn't match, don't continue
            if "token" not in data:
                raise ValueError("token missing from request")
            if not data["token"] == config.PUBSUB_VERIFICATION_TOKEN:
                raise ValueError("token from request doesn't match, received: {}".format(data["token"]))

            # if the project has been passed in, use that, otherwise use default project of App Engine app
            if "project_id" not in data:
                project_id = project_id=app_identity.get_application_id()
            else:
                project_id = data["project_id"]
            message_to_publish["project_id"] = project_id

            # if the alignment_period is supplied, use that, otherwise use default
            if "aggregation_alignment_period" not in data:
                aggregation_alignment_period = config.AGGREGATION_ALIGNMENT_PERIOD
            else:
                aggregation_alignment_period = data["aggregation_alignment_period"]
                pattern = re.compile("^\d{1,}s+$")
                matched = pattern.match(aggregation_alignment_period)
                if not matched:
                    raise ValueError("aggregation_alignment_period needs to be digits followed by an 's' such as 3600s, received: {}".format(aggregation_alignment_period))
            alignment_seconds = int(aggregation_alignment_period[:len(aggregation_alignment_period)-1])
            if alignment_seconds < 60:
                raise ValueError("aggregation_alignment_period needs to be more than 60s, received: {}".format(aggregation_alignment_period)) 
            message_to_publish["aggregation_alignment_period"] = aggregation_alignment_period

            # get the App Engine default bucket name to store a GCS file with last end_time
            bucket_name = os.environ.get('BUCKET_NAME',
                app_identity.get_default_gcs_bucket_name()
            )

            # Calculate the end_time first
            if "end_time" not in data:
                # the end_time should be set here for all metrics in the batch
                # setting later in the architecture would mean that the end_time may vary
                end_time = datetime.now()
                end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                end_time_str = data["end_time"]
                matched = check_date_format(end_time_str)
                if not matched:
                    raise ValueError("end_time needs to be in the format 2019-02-08T14:00:00.311635Z, received: {}".format(end_time_str))
            message_to_publish["end_time"] = end_time_str

            # if the start_time is supplied, use the previous end_time 
            sent_in_start_time_flag = False
            if "start_time" not in data:
                start_time_str = get_last_end_time(project_id, bucket_name)
                # if the file hasn't been found, then start 1 alignment period in the past
                if not start_time_str:
                    start_time_str = set_last_end_time(project_id, bucket_name, end_time_str, (alignment_seconds * -1))
                    #raise ValueError("start_time couldn't be read from GCS, received: {}".format(start_time_str))
                logging.debug("start_time_str: {}, end_time_str: {}".format(start_time_str, end_time_str))
            else:
                sent_in_start_time_flag = True
                start_time_str = data["start_time"]
                matched = check_date_format(start_time_str)
                if not matched:
                    raise ValueError("start_time needs to be in the format 2019-02-08T14:00:00.311635Z, received: {}".format(start_time_str))
            message_to_publish["start_time"] = start_time_str

            # Create a unique identifier for this batch
            batch_id = get_batch_id()
            logging.debug("batch_id: {}".format(batch_id))

            # Publish the messages to Pub/Sub
            logging.info("Running with input parameters - {}".format(json.dumps(message_to_publish, sort_keys=True, indent=4)))

            metadata = {
                "batch_id": batch_id,
                "message_id": message_id,
                "batch_start_time": publish_time
            }
            if config.WRITE_BQ_STATS_FLAG:
                write_input_parameters_to_bigquery(project_id, metadata, message_to_publish)
            stats = get_and_publish_metrics(message_to_publish, metadata)
            logging.debug("Stats are {}".format(json.dumps(stats)))

            """ Write the late end_time_str to GCS to use in a subsequent run,
            but only if the start_time was not sent in. If the start_time is 
            supplied, then we consider that an ad hoc run, and won't set the
            previous end_time
            """
            if not sent_in_start_time_flag:
                set_last_end_time(project_id, bucket_name, end_time_str, 1)

            # Write the stats to custom monitoring metrics
            if config.WRITE_MONITORING_STATS_FLAG:
                write_stats(stats, project_id, batch_id)

            self.response.write(stats)
        except KeyError as ke:
            logging.error("Key Error: {}".format(ke))
            self.response.write(ke)
        except ValueError as ve:
            logging.error("Value Error: {}".format(ve))
            self.response.write(ve)
        except Exception as e:
            logging.error("Error: {}".format(e))
            self.response.write(e)

        self.response.status = 200


app = webapp2.WSGIApplication([
    ('/_ah/push-handlers/receive_message', ReceiveMessage)
], debug=True)
