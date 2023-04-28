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

import pytest
from main import app as flask_app

import main
import config
import base64
import json


@pytest.fixture
def app():
    yield flask_app


@pytest.fixture
def client(app):
    return app.test_client()


def test_check_date_format():
    """Test the check_date_format function"""
    results = main.check_date_format("23232")
    assert results is None
    results = main.check_date_format("2019-02-08T14:00:00.311635Z")
    assert results is not None


def test_post_empty_data(app, client):
    """Test sending an empty message"""
    response = client.post("/push-handlers/receive_messages")
    assert response.status_code == 500
    assert response.get_data(as_text=True) == "No request data received"


def test_incorrect_aggregation_alignment_period_post(app, client):
    mimetype = "application/json"
    headers = {
        "Content-Type": mimetype,
        "Accept": mimetype,
    }
    """Test sending incorrect aggregation_alignment_period as input"""
    request = build_request(aggregation_alignment_period="12")
    response = client.post(
        "/push-handlers/receive_messages", data=json.dumps(request).encode("utf-8"), headers=headers
    )
    assert response.status_code == 500
    assert (
        response.get_data(as_text=True)
        == "aggregation_alignment_period needs to be digits followed by an 's' such as 3600s, received: 12"
    )

    request = build_request(aggregation_alignment_period="12s")
    response = client.post(
        "/push-handlers/receive_messages", data=json.dumps(request).encode("utf-8"), headers=headers
    )
    assert response.status_code == 500
    assert (
        response.get_data(as_text=True)
        == "aggregation_alignment_period needs to be more than 60s, received: 12s",
    )


def test_exclusions_check():
    """Test the exclusion logic"""
    assert (
        main.check_exclusions({"type": "aws.googleapis.com/flex/cpu/utilization"})
        == False
    ), "This should be excluded"
    assert (
        main.check_exclusions({"type": "appengine.googleapis.com/flex/cpu/utilization"})
        == True
    ), "This should not be excluded"


def test_incorrect_token_post(app, client):
    """Test sending an incorrect token"""
    request = build_request(token="incorrect_token")
    mimetype = "application/json"
    headers = {
        "Content-Type": mimetype,
        "Accept": mimetype,
    }
    response = client.post("/push-handlers/receive_messages", data=json.dumps(request), headers=headers)
    assert response.status_code == 500


def build_request(
    token=config.PUBSUB_VERIFICATION_TOKEN,
    aggregation_alignment_period="3600s",
):
    """Build a Pub/Sub message as input"""

    payload = {
        "token": token,
        "aggregation_alignment_period": aggregation_alignment_period,
    }
    request = {
        "message": {
            "data": base64.b64encode(json.dumps(payload).encode("utf-8")).decode()
        }
    }
    return request
