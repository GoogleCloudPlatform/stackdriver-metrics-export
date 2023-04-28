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


def test_aligner_reducer_values():
    """Test the get_aligner_reducer() function logic"""
    crossSeriesReducer, perSeriesAligner = main.get_aligner_reducer(
        config.GAUGE, config.BOOL
    )
    assert crossSeriesReducer == config.REDUCE_MEAN
    assert perSeriesAligner == config.ALIGN_FRACTION_TRUE

    crossSeriesReducer, perSeriesAligner = main.get_aligner_reducer(
        config.GAUGE, config.INT64
    )
    assert crossSeriesReducer == config.REDUCE_SUM
    assert perSeriesAligner == config.ALIGN_SUM

    crossSeriesReducer, perSeriesAligner = main.get_aligner_reducer(
        config.DELTA, config.INT64
    )
    assert crossSeriesReducer == config.REDUCE_SUM
    assert perSeriesAligner == config.ALIGN_SUM

    crossSeriesReducer, perSeriesAligner = main.get_aligner_reducer(
        config.CUMULATIVE, config.INT64
    )
    assert crossSeriesReducer == config.REDUCE_SUM
    assert perSeriesAligner == config.ALIGN_DELTA


def test_post_empty_data(app, client):
    """Test sending an empty message"""
    response = client.post("/push-handlers/receive_messages")
    assert response.status_code == 500
    assert response.get_data(as_text=True) == "No request data received"


def test_incorrect_token_post(app, client):
    """Test sending an incorrect token"""
    request = build_request(token="incorrect_token")
    mimetype = "application/json"
    headers = {
        "Content-Type": mimetype,
        "Accept": mimetype,
    }
    response = client.post(
        "/push-handlers/receive_messages", data=json.dumps(request), headers=headers
    )
    assert response.status_code == 500


def build_request(
    token=config.PUBSUB_VERIFICATION_TOKEN,
    batch_id="12h3eldjhwuidjwk222dwd09db5zlaqs",
    metric_type="bigquery.googleapis.com/query/count",
    metric_kind=config.GAUGE,
    value_type=config.INT64,
    start_time="2019-02-18T13:00:00.311635Z",
    end_time="2019-02-18T14:00:00.311635Z",
    aggregation_alignment_period="3600s",
):
    """Build a request to submit"""

    payload = {
        "metric": {
            "type": metric_type,
            "metricKind": metric_kind,
            "valueType": value_type,
        },
        "start_time": start_time,
        "end_time": end_time,
        "aggregation_alignment_period": aggregation_alignment_period,
    }
    request = {
        "message": {
            "attributes": {"batch_id": batch_id, "token": token},
            "data": base64.b64encode(json.dumps(payload).encode("utf-8")).decode(),
        }
    }
    return request
