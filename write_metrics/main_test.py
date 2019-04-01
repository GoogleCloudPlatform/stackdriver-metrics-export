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

import webtest
import unittest
import main
import config
import base64
import json
from google.appengine.ext import testbed

class AppTest(unittest.TestCase):
    
    def setUp(self):
        """ Set-up the webtest app
        """
        self.app = webtest.TestApp(main.app)    
        self.batch_id = "R1HIA55JB5DOQZM8R53OKMCWZ5BEQKUJ"

    def test_post_empty_data(self):
        """ Test sending an empty message
        """
        response = self.app.post('/_ah/push-handlers/receive_message')
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.body, "No request body received")
        self.assertRaises(ValueError)

    def test_incorrect_token_post(self):
        """ Test sending an incorrect token
        """         
        request = self.build_request(token="incorrect_token")
        response = self.app.post('/_ah/push-handlers/receive_message',json.dumps(request).encode('utf-8'),content_type="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertRaises(ValueError)

    def test_correct_labels(self):
        """ Test whether the correct labels are extracted from the metric API responses
        """
        timeseries = self.build_timeseries()

        metric_labels_list = main.get_labels(timeseries["metric"],"labels")        
        expected_metric_labels_list = self.build_metric_labels()
        self.assertEqual(sorted(metric_labels_list), sorted(expected_metric_labels_list))

        resource_labels_list = main.get_labels(timeseries["resource"],"labels")
        expected_resource_labels_list = self.build_resource_labels()
        self.assertEqual(sorted(resource_labels_list), sorted(expected_resource_labels_list))

        user_labels_list = main.get_labels(self.build_user_labels_request(),"userLabels")
        expected_user_labels_list = self.build_expected_user_labels_response()
        self.assertEqual(sorted(user_labels_list), sorted(expected_user_labels_list))

        system_labels_list = main.get_system_labels(self.build_user_labels_request(),"systemLabels")
        expected_system_labels_list = self.build_expected_system_labels_response()
        self.assertEqual(sorted(system_labels_list), sorted(expected_system_labels_list))    

    def test_correct_build_distribution_values(self):
        """ Test whether the correct distribution values are built given a timeseries input
        """
        timeseries_with_distribution_values = self.build_distribution_value()

        distribution_value = main.build_distribution_value(timeseries_with_distribution_values["points"][0]["value"]["distributionValue"])
        expected_distribution_value = self.build_expected_distribution_value()
        self.assertEqual(distribution_value,expected_distribution_value)

    def test_correct_build_row(self):
        """ Test whether the correct JSON object is created for insert into BigQuery given a timeseries input
        """
        timeseries = self.build_timeseries()
        bq_body = main.build_rows(timeseries,self.batch_id)
        
        bq_expected_response = self.build_expected_bq_response()
        self.assertEqual(bq_body, bq_expected_response)

    def build_timeseries(self):
        """ Build a timeseries object to use as input
        """
        timeseries = {
            "metricKind": "DELTA", 
            "metric": {
                "labels": {
                    "response_code": "0"}, 
                    "type": "agent.googleapis.com/agent/request_count"
                }, 
                "points": [
                    {
                        "interval": {
                            "endTime": "2019-02-18T22:09:53.939194Z", 
                            "startTime": "2019-02-18T21:09:53.939194Z"
                        }, 
                        "value": {
                            "int64Value": "62"
                        }
                    }, 
                    {
                        "interval": {
                            "endTime": "2019-02-18T21:09:53.939194Z", 
                            "startTime": "2019-02-18T20:09:53.939194Z"
                        }, 
                        "value": {
                            "int64Value": "61"
                            }
                    }
                ], 
                "resource": {
                    "labels": {
                        "instance_id": "9113659852587170607", 
                        "project_id": "YOUR_PROJECT_ID", 
                        "zone": "us-east4-a"
                    }, 
                    "type": "gce_instance"
                }, 
                "valueType": "INT64"
            }

        return timeseries

    def build_expected_bq_response(self):
        """ Build the expected BigQuery insert JSON object 
        """
        response = [
            { 
                "json": {
                    "batch_id": self.batch_id, 
                    "metric": {
                        "labels": [
                            {
                                "key": "response_code", 
                                "value": "0"
                            }
                        ], 
                        "type": "agent.googleapis.com/agent/request_count"
                    }, 
                    "metric_kind": "DELTA", 
                    "point": {
                        "interval": {
                            "end_time": "2019-02-18T22:09:53.939194Z", 
                            "start_time": "2019-02-18T21:09:53.939194Z"
                        }, 
                        "value": {
                            "int64_value": "62"
                        }
                    }, 
                    "resource": {
                        "labels": [
                            {
                                "key": "instance_id", 
                                "value": "9113659852587170607"
                            }, 
                            {
                                "key": "project_id", 
                                "value": "YOUR_PROJECT_ID"
                            }, 
                            {
                                "key": "zone", 
                                "value": "us-east4-a"
                            }
                        ], 
                        "type": "gce_instance"
                    }, 
                    "value_type": "INT64"
                }
            }, 
            {
                "json": {
                    "batch_id": self.batch_id, 
                    "metric": {
                        "labels": [
                            {
                                "key": "response_code", 
                                "value": "0"
                            }
                        ], 
                        "type": "agent.googleapis.com/agent/request_count"
                    }, 
                    "metric_kind": "DELTA", 
                    "point": {
                        "interval": {
                            "end_time": "2019-02-18T21:09:53.939194Z", 
                            "start_time": "2019-02-18T20:09:53.939194Z"
                        }, 
                        "value": {
                            "int64_value": "61"
                        }
                    }, 
                    "resource": {
                        "labels": [
                            {
                                "key": "instance_id", 
                                "value": "9113659852587170607"
                            }, 
                            {
                                "key": "project_id", 
                                "value": "YOUR_PROJECT_ID"
                            }, 
                            {
                                "key": "zone", 
                                "value": "us-east4-a"
                            }
                        ], 
                        "type": "gce_instance"
                    }, 
                    "value_type": "INT64"
                }
            }
        ]
        return response

    def build_metric_labels(self):
        """ Build the expected metric labels list
        """
        response = [ 
            {
                "key": "response_code", 
                "value": "0"
            }
        ]
        return response

    def build_resource_labels(self):
        """ Build the expected resource labels list
        """
        response = [ 
            {
                "key": "instance_id", 
                "value": "9113659852587170607"
            }, 
            {
                "key": "project_id", 
                "value": "YOUR_PROJECT_ID"
            }, 
            {
                "key": "zone", 
                "value": "us-east4-a"
            }
        ]
        return response
    
    def build_request(self,token=config.PUBSUB_VERIFICATION_TOKEN):
        """ Build a Pub/Sub message as input
        """
        payload = {
            "metricKind": "DELTA", 
            "metric": {
                "labels": {
                    "response_code": "0"
                }, 
                "type": "agent.googleapis.com/agent/request_count"
            }, 
            "points": [
                    {
                        "interval": {"endTime": "2019-02-18T22:09:53.939194Z", "startTime": "2019-02-18T21:09:53.939194Z"}, 
                        "value": {"int64Value": "62"}
                    }, 
                    {
                        "interval": {"endTime": "2019-02-18T21:09:53.939194Z", "startTime": "2019-02-18T20:09:53.939194Z"}, 
                        "value": {"int64Value": "61"}
                    }
            ], 
            "resource": {
                "labels": {
                    "instance_id": "9113659852587170607", 
                    "project_id": "YOUR_PROJECT_ID", 
                    "zone": "us-east4-a"
                }, 
                "type": "gce_instance"
            }, 
            "valueType": "INT64"
        }
        request = {
            "message": 
                {
                    "attributes": {
                        "batch_id": self.batch_id,
                        "token": token
                    },
                    "data": base64.b64encode(json.dumps(payload))
                }
            
        }
        return request

    def build_user_labels_request(self):
        """ Build the JSON input for the userLabels and systemLabels 
        """
        request = {
            "systemLabels": {
                "name": "appName",
                "list_name": [ "a","b","c"],
                "boolean_value": False
            },
            "userLabels": {
                "key1": "value1",
                "key2": "value2"
            }
        }
        return request

    def build_expected_system_labels_response(self):
        """ Build the expected system labels list
        """
        labels = [
            {
                "key": "name",
                "value": "appName"
            },
            {
                "key": "boolean_value",
                "value": "False"
            },
            {
                "key": "list_name",
                "value": "a"
            },
            {
                "key": "list_name",
                "value": "b"
            },
            {
                "key": "list_name",
                "value": "c"
            }
        ]
        return labels
    
    def build_expected_user_labels_response(self):
        """ Build the expected user labels list
        """
        labels = [
            {
                "key": "key1",
                "value": "value1"
            },
            {
                "key": "key2",
                "value": "value2"
            }
        ]
        return labels
    
    def build_distribution_value(self):
        """ Build the expected JSON object input for the distribution values test
        """
        timeseries = {
            "metricKind": "DELTA", 
            "metric": {
                "type": "serviceruntime.googleapis.com/api/response_sizes"
            }, 
            "points": [
                {
                    "interval": {
                        "endTime": "2019-02-19T04:00:00.841487Z", 
                        "startTime": "2019-02-19T03:00:00.841487Z"
                    }, 
                "value": {
                    "distributionValue": {
                        "count": "56", 
                        "mean": 17,
                        "sumOfSquaredDeviation": 1.296382457204002e-25,
                        "bucketCounts": ["56"], 
                        "bucketOptions": {
                            "exponentialBuckets": {
                                "scale": 1, 
                                "growthFactor": 10, 
                                "numFiniteBuckets": 8
                                }
                            }
                        }
                    }
                }
            ], 
            "resource": {
                "labels": {
                    "service": "monitoring.googleapis.com", 
                    "credential_id": "serviceaccount:106579349769273816070", 
                    "version": "v3", 
                    "location": "us-central1", 
                    "project_id": "ms-demo-app01", 
                    "method": "google.monitoring.v3.MetricService.ListMetricDescriptors"
                }, 
            "type": "consumed_api"
            }, 
            "valueType": "DISTRIBUTION"}
        return timeseries
    
    def build_expected_distribution_value(self):
        """ Build the expected JSON object for the distribution values test
        """
        distribution_value = {
            "count": 56,
            "mean": 17.0,
            "sumOfSquaredDeviation": 0.0,
            "bucketOptions": {
                "exponentialBuckets": {
                    "numFiniteBuckets": 8,
                    "growthFactor": 10.0,
                    "scale": 1
                }
            },
            "bucketCounts": {
                "value": [56]
            }
        }
        return distribution_value