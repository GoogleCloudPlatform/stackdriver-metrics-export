# Reference Implementation for Stackdriver Metric Export
This implementation is meant to demonstrate how to use the projects.timeseries.list API, PubSub and BigQuery to store aggregated Stackdriver Monitoring 
metrics for long-term analysis in BigQuery.

# Deployment Instructions
1. Clone the [source repo]()
```sh
git clone https://github.com/GoogleCloudPlatform/stackdriver-metrics-export
cd stackdriver-metrics-export
```

2. Create the BigQuery tables
Create a Dataset and then a table using the schema JSON files
```sh
bq mk metric_export
bq mk --table --time_partitioning_type=DAY metric_export.sd_metrics_export_fin ./bigquery_schema.json
bq mk --table --time_partitioning_type=DAY metric_export.sd_metrics_stats ./bigquery_schema_stats_table.json
bq mk --table metric_export.sd_metrics_params ./bigquery_schema_params_table.json
```

3. Replace the JSON token in the config.py files
Generate a new token and then replace that token in the each of config.py files. Use this same token in the Cloud Scheduler.
```sh
TOKEN=$(python -c "import uuid;  msg = uuid.uuid4(); print msg")
sed -i s/16b2ecfb-7734-48b9-817d-4ac8bd623c87/$TOKEN/g stackdriver-metrics-export/list_metrics/config.py
sed -i s/16b2ecfb-7734-48b9-817d-4ac8bd623c87/$TOKEN/g stackdriver-metrics-export/get_timeseries/config.py
sed -i s/16b2ecfb-7734-48b9-817d-4ac8bd623c87/$TOKEN/g stackdriver-metrics-export/write_metrics/config.py
```

4. Deploy the App Engine apps
Run a gcloud app create if you don't already have an App Engine app in your project and remove the service: list-metrics from app.yaml

```sh
cd list_metrics
pip install -t lib -r requirements.txt
echo "y" | gcloud app deploy

cd ../get_timeseries
pip install -t lib -r requirements.txt
echo "y" | gcloud app deploy

cd ../write_metrics
pip install -t lib -r requirements.txt
echo "y" | gcloud app deploy
```


```


4. Create the Pub/Sub topics and subscriptions after setting YOUR_PROJECT_ID

If you already have a default App Engine app in your project, enter the following command.

```sh
export LIST_METRICS_URL=$(gcloud app browse -s list-metrics)
```
if this was your first App Engine app in your project, enter the following command. 

```sh
export LIST_METRICS_URL=$(gcloud app browse)
```

Now, get the get_timeseries and write_metrics URLs and create the Pub/Sub topics and subscriptions

```sh
export GET_TIMESERIES_URL=$(gcloud app browse -s get-timeseries)
export WRITE_METRICS_URL=$(gcloud app browse -s write-metrics)

gcloud pubsub topics create metrics_export_start
gcloud pubsub subscriptions create metrics_export_start_sub --topic metrics_export_start --ack-deadline=60 --message-retention-duration=10m --push-endpoint="$LIST_METRICS_URL/_ah/push-handlers/receive_message"

gcloud pubsub topics create metrics_list
gcloud pubsub subscriptions create metrics_list_sub --topic metrics_list --ack-deadline=60 --message-retention-duration=30m --push-endpoint="$GET_TIMESERIES_URL/_ah/push-handlers/receive_message"

gcloud pubsub topics create write_metrics
gcloud pubsub subscriptions create write_metrics_sub --topic write_metrics --ack-deadline=60 --message-retention-duration=30m  --push-endpoint="$WRITE_METRICS_URL/_ah/push-handlers/receive_message"
``` 


5. Test the app by sending a PubSub message to the metrics_export_start topic
```sh
gcloud pubsub topics publish metrics_export_start --message "{\"token\": \"$TOKEN\"}" 
```

You can send in all of the parameters using the following command
```sh
gcloud pubsub topics publish metrics_export_start --message "{\"token\": \"$TOKEN\"}, \"start_time\": \"2019-03-13T17:30:00.000000Z\", \"end_time\":\"2019-03-13T17:40:00.000000Z\",\"aggregation_alignment_period\":\"3600s\"}"
```

6. Verify that the app is working appropriately by running the end-to-end testing

Configure your project_id and lookup the batch_id in the config.py file.
```sh
cd test
export PROJECT_ID=$(gcloud config get-value project)
export TIMESTAMP=$(date -d "-2 hour"  +%Y-%m-%dT%k:%M:00Z)
export BATCH_ID=$(gcloud logging read "resource.type=\"gae_app\" AND resource.labels.module_id=\"list-metrics\" AND logName=\"projects/$PROJECT_ID/logs/appengine.googleapis.com%2Frequest_log\" AND protoPayload.line.logMessage:\"batch_id:\" AND timestamp >= \"$TIMESTAMP\"" --limit 1 --format json | grep "batch_id:" | awk '{ print substr($3,1,32); }')
sed -i s/YOUR_PROJECT_ID/$PROJECT_ID/g config.py
sed -i s/R8BK5S99QU4ZZOGCR1UDPWVH6LPKI5QU/$BATCH_ID/g config.py

python end_to_end_test_run.py
```
