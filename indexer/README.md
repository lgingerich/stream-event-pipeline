# Indexer Component

## Purpose

The `indexer` service is responsible for consuming raw data streams (e.g., from a blockchain), processing this data, and producing standardized messages onto a Kafka topic for downstream consumption.

## How it Works

1.  Connects to the specified raw data source.
2.  Processes incoming data records.
3.  Transforms the data into the required message format.
4.  Publishes the messages to the designated Kafka topic.

## Running

This service is typically run as part of the main `docker-compose` setup. Refer to the [main README](../README.md) for instructions.
