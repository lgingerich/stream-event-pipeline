# Persistence Component

## Purpose

The `persistence` service consumes processed messages from a Kafka topic (produced by the `indexer`) and saves the data into a persistent storage solution, specifically Postgres in this setup.

## How it Works

1.  Connects to the specified Kafka topic as a consumer.
2.  Receives messages containing processed data.
3.  Connects to the Postgres database.
4.  Transforms/maps the message data to the appropriate database schema.
5.  Saves the data to the relevant Postgres tables.

## Running

This service is typically run as part of the main `docker-compose` setup. Refer to the [main README](../README.md) for instructions.
