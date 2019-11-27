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

import webapp2
import config
import os
import json
import logging
from datetime import datetime 
import cloudstorage as gcs
from cloudstorage import NotFoundError
from google.appengine.api import app_identity

class ReceiveStart(webapp2.RequestHandler):
    def set_last_end_time(self, bucket_name):
        """ Write the end_time as a string value in a JSON object in GCS. 
            This file is used to remember the last end_time in case one isn't provided
        """
        project_id = app_identity.get_application_id()
        end_time = datetime.now()
        end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        end_time_str_json = {
            "end_time": end_time_str
        }
        write_retry_params = gcs.RetryParams(backoff_factor=1.1)
        gcs_file = gcs.open('/{}/{}'.format(
            bucket_name, '{}.{}'.format(
                project_id,
                config.LAST_END_TIME_FILENAME)),
                            'w',
                            content_type='text/plain',
                            retry_params=write_retry_params)
        gcs_file.write(json.dumps(end_time_str_json))
        gcs_file.close()

    def get(self):
        last_end_time_str = ""
        try:
            # get the App Engine default bucket name to store a GCS file with last end_time
            project_id = app_identity.get_application_id()
            bucket_name = os.environ.get('BUCKET_NAME',
                app_identity.get_default_gcs_bucket_name()
            )

            gcs_file = gcs.open('/{}/{}'.format(
                bucket_name, '{}.{}'.format(
                    project_id,
                    config.LAST_END_TIME_FILENAME)))
            contents = gcs_file.read()
            logging.debug("GCS FILE CONTENTS: {}".format(contents))
            json_contents = json.loads(contents) 
            last_end_time_str = json_contents["end_time"]
            gcs_file.close()
        except NotFoundError as nfe:
            logging.error("Missing file when reading from GCS: {}".format(nfe))
            last_end_time_str = None
        except Exception as e:
            logging.error("Received error when reading from GCS: {}".format(e))
            last_end_time_str = None

        try:
            if not last_end_time_str:
                self.set_last_end_time(bucket_name)
        except NotFoundError as nfe:
            logging.error("Missing file when writing to GCS: {}".format(nfe))
            last_end_time_str = None
        except Exception as e:
            logging.error("Received error when writing to GCS: {}".format(e))
            last_end_time_str = None
    
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.status = 200


app = webapp2.WSGIApplication([
    ('/_ah/start', ReceiveStart)
], debug=True)
