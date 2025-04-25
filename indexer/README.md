# Indexer Component

## Purpose

The `indexer` service is responsible for consuming raw data streams, processing this data, and producing standardized messages onto a Kafka topic for downstream consumption.

## How it Works

1.  Connects to the specified raw data source.
2.  Processes incoming data records.
3.  Decodes event logs according contract ABIs included in [./abi](./abi).
4.  Transforms the data into the required message format.
5.  Publishes the messages to the designated Kafka topic.

## Running

This service is typically run as part of the main `docker-compose` setup. Refer to the [main README](../README.md) for instructions.
