# Runbook

## MVP Flow

1. Collector receives raw WebSocket messages.
2. Collector publishes raw messages to Kafka.
3. Consumer stores raw messages in S3 Bronze.
4. Clean Lambda writes Silver trade and daily Parquet outputs.
5. Signal Lambda writes Gold signal history and publishes alerts.

## Reprocessing

Reprocessing should start from the Bronze raw logs because they preserve the original WebSocket messages.

