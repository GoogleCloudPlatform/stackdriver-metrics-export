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
    
    def test_aligner_reducer_values(self):
        """ Test the get_aligner_reducer() function logic
        """
        crossSeriesReducer, perSeriesAligner = main.get_aligner_reducer(config.GAUGE,config.BOOL)
        self.assertEqual(crossSeriesReducer, config.REDUCE_MEAN)
        self.assertEqual(perSeriesAligner, config.ALIGN_FRACTION_TRUE)

        crossSeriesReducer, perSeriesAligner = main.get_aligner_reducer(config.GAUGE,config.INT64)
        self.assertEqual(crossSeriesReducer, config.REDUCE_SUM)
        self.assertEqual(perSeriesAligner, config.ALIGN_SUM)

        crossSeriesReducer, perSeriesAligner = main.get_aligner_reducer(config.DELTA,config.INT64)
        self.assertEqual(crossSeriesReducer, config.REDUCE_SUM)
        self.assertEqual(perSeriesAligner, config.ALIGN_SUM)

        crossSeriesReducer, perSeriesAligner = main.get_aligner_reducer(config.CUMULATIVE,config.INT64)
        self.assertEqual(crossSeriesReducer, config.REDUCE_SUM)
        self.assertEqual(perSeriesAligner, config.ALIGN_DELTA)

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

    def build_request(self,
        token=config.PUBSUB_VERIFICATION_TOKEN,
        batch_id="12h3eldjhwuidjwk222dwd09db5zlaqs",
        metric_type="bigquery.googleapis.com/query/count",
        metric_kind=config.GAUGE,
        value_type=config.INT64,
        start_time="2019-02-18T13:00:00.311635Z",
        end_time="2019-02-18T14:00:00.311635Z",
        aggregation_alignment_period="3600s"):
        """ Build a request to submit 
        """

        payload = {
            "metric": {
                "type": metric_type,
                "metricKind": metric_kind,
                "valueType": value_type
            },
            "start_time": start_time,
            "end_time": end_time,
            "aggregation_alignment_period": aggregation_alignment_period
        }
        request = {
            "message": 
                {
                    "attributes": {
                        "batch_id": batch_id,
                        "token": token
                    },
                    "data": base64.b64encode(json.dumps(payload))
                }
            
        }
        return request