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


class AppTest(unittest.TestCase):
    def setUp(self):
        """ Set-up the webtest app
        """
        self.app = webtest.TestApp(main.app)
    
    def test_check_date_format(self):
        """ Test the check_date_format function
        """
        results = main.check_date_format("23232")
        self.assertIsNone(results)
        results = main.check_date_format("2019-02-08T14:00:00.311635Z")
        self.assertIsNotNone(results)

    def test_post_empty_data(self):
        """ Test sending an empty message
        """
        response = self.app.post('/_ah/push-handlers/receive_message')
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.body, "No request body received")

    def test_incorrect_aggregation_alignment_period_post(self):   
        """ Test sending incorrect aggregation_alignment_period as input
        """
        request = self.build_request(aggregation_alignment_period = "12")
        response = self.app.post('/_ah/push-handlers/receive_message',json.dumps(request).encode('utf-8'),content_type="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertRaises(ValueError)
        self.assertEqual(response.body, "aggregation_alignment_period needs to be digits followed by an 's' such as 3600s, received: 12")

        request = self.build_request(aggregation_alignment_period = "12s")
        response = self.app.post('/_ah/push-handlers/receive_message',json.dumps(request).encode('utf-8'),content_type="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertRaises(ValueError)
        self.assertEqual(response.body, "aggregation_alignment_period needs to be more than 60s, received: 12s")


    def test_exclusions_check(self):
        """ Test the exclusion logic
        """
        assert main.check_exclusions("aws.googleapis.com/flex/cpu/utilization") == False, "This should be excluded"
        assert main.check_exclusions("appengine.googleapis.com/flex/cpu/utilization") == True, "This should not be excluded"



    def test_incorrect_token_post(self): 
        """ Test sending an incorrect token
        """
        request = self.build_request(token="incorrect_token")
        response = self.app.post('/_ah/push-handlers/receive_message',json.dumps(request).encode('utf-8'),content_type="application/json")
        self.assertEqual(response.status_int, 200)
        self.assertRaises(ValueError)
    
    def build_request(self,token=config.PUBSUB_VERIFICATION_TOKEN,aggregation_alignment_period="3600s"):
        """ Build a Pub/Sub message as input
        """

        payload = {
            "token": token,
            "aggregation_alignment_period": aggregation_alignment_period
        }
        request = {
            "message": 
                {
                    "data": base64.b64encode(json.dumps(payload))
                }
            
        }
        return request