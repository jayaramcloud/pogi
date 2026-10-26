---
title: Read logs and metrics
description: Write log events and metrics, read them back and trigger an alarm with CloudWatch and Cloud Logging against pogi.
---

# Read logs and metrics

pogi keeps the logs and metrics your code writes, so you can read them back and test alarms. You'll write log events, read them with a filter, raise a CloudWatch alarm, and do the same for logs on Google Cloud.

## Before you start

1. Start pogi in another terminal:

   ```bash
   pogi start
   ```

2. Load the settings that point your tools at pogi:

   ```bash
   eval "$(pogi env)"
   ```

You need the AWS CLI for the CloudWatch sections and `gcloud` for the Cloud Logging section. No Docker needed.

## AWS: CloudWatch Logs

You'll make a log group (a named collection of logs) with one stream, write two events, and read them back.

1. Create the group and the stream. Neither command prints anything:

   ```bash
   aws logs create-log-group --log-group-name /demo/app
   aws logs create-log-stream --log-group-name /demo/app --log-stream-name web-1
   ```

2. Write two events. A timestamp is in milliseconds since 1970:

   ```bash
   NOW=$(($(date +%s) * 1000))
   aws logs put-log-events --log-group-name /demo/app --log-stream-name web-1 \
     --log-events timestamp=$NOW,message="order 1 created" timestamp=$((NOW + 1)),message="ERROR payment failed"
   ```

   ```text title="Output"
   {
       "nextSequenceToken": "1"
   }
   ```

3. Read the events back:

   ```bash
   aws logs get-log-events --log-group-name /demo/app --log-stream-name web-1 --query 'events[].message'
   ```

   ```text title="Output"
   [
       "order 1 created",
       "ERROR payment failed"
   ]
   ```

4. Search the whole group for events that match a pattern:

   ```bash
   aws logs filter-log-events --log-group-name /demo/app --filter-pattern ERROR --query 'events[].message'
   ```

   ```text title="Output"
   [
       "ERROR payment failed"
   ]
   ```

## AWS: CloudWatch metrics and alarms

You'll make an alarm that fires when `Orders` adds up to more than 2 in a minute, then write a metric value that crosses it.

1. Create the alarm. It prints nothing:

   ```bash
   aws cloudwatch put-metric-alarm --alarm-name too-many-orders --namespace Demo --metric-name Orders \
     --statistic Sum --period 60 --evaluation-periods 1 --threshold 2 --comparison-operator GreaterThanThreshold
   ```

2. Write a value of 5. It prints nothing:

   ```bash
   aws cloudwatch put-metric-data --namespace Demo --metric-name Orders --value 5 --unit Count
   ```

3. Wait until the alarm fires, then show it. The alarm can take up to a minute to fire:

   ```bash
   for i in $(seq 1 30); do
     STATE=$(aws cloudwatch describe-alarms --alarm-names too-many-orders --query 'MetricAlarms[0].StateValue' --output text)
     [ "$STATE" = ALARM ] && break
     sleep 5
   done
   aws cloudwatch describe-alarms --alarm-names too-many-orders --query 'MetricAlarms[].[AlarmName,StateValue,StateReason]' --output text
   ```

   ```text title="Output"
   too-many-orders	ALARM	Threshold Crossed: 1 out of the last 1 datapoints [5] breached the threshold (GreaterThanThreshold 2).
   ```

4. List the metric you wrote:

   ```bash
   aws cloudwatch list-metrics --namespace Demo --query 'Metrics[].MetricName' --output text
   ```

   ```text title="Output"
   Orders
   ```

An alarm can also notify an SNS topic or run a Lambda function when it fires. Set that with `--alarm-actions` on `put-metric-alarm`.

## Google Cloud: Cloud Logging

You'll write two log entries and read back the ones at ERROR or above.

1. Write an entry at each severity:

   ```bash
   gcloud logging write demo-log "order 1 created" --severity=INFO
   gcloud logging write demo-log "payment failed" --severity=ERROR
   ```

   ```text title="Output"
   Created log entry.
   Created log entry.
   ```

2. Read the errors. The filter uses the Cloud Logging query language:

   ```bash
   gcloud logging read 'logName="projects/pogi/logs/demo-log" AND severity>=ERROR' --limit=5 --format='value(severity,textPayload)'
   ```

   ```text title="Output"
   ERROR	payment failed
   ```

3. List the logs in the project:

   ```bash
   gcloud logging logs list
   ```

   ```text title="Output"
   NAME
   projects/pogi/logs/demo-log
   ```

## Limits

- CloudWatch Logs: Insights does not run `stats` or `parse` yet. Metric filters read JSON fields only, not space-delimited fields such as `$name`.
- CloudWatch metrics: composite alarms are not evaluated yet.
- Cloud Logging: log views, links, analytics and log scopes are not supported yet.
- Azure Monitor is left out.

See the [CloudWatch Logs and Metrics](../reference/coverage.md#aws) and [Cloud Logging](../reference/coverage.md#google-cloud) rows in the coverage page.

## See also

- [Connect your tools](connect-your-tools.md)
- [Run functions and HTTP APIs](run-functions-and-http-apis.md)
- [Coverage](../reference/coverage.md)
