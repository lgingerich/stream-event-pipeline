from dotenv import load_dotenv
from kafka import KafkaProducer, errors as KafkaErrors
from loguru import logger
import os
import json
from typing import List, Dict

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
if not KAFKA_BOOTSTRAP_SERVERS or not KAFKA_TOPIC:
    raise Exception("KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC must be set")

logger.info(f"Attempting to connect to Kafka brokers at: {KAFKA_BOOTSTRAP_SERVERS}")
logger.info(f"Target Kafka topic: {KAFKA_TOPIC}")

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    logger.info("Successfully connected to Kafka.")
except KafkaErrors.NoBrokersAvailable as e:
    logger.error(f"Failed to connect to Kafka: {e}")
    raise e

# Ensure producer was initialized
if producer is None:
    raise Exception("KafkaProducer could not be initialized.")

def produce_message(message: List[Dict]):
    try:
        # Serialize the message to a JSON string
        message_json = json.dumps(message)
        # Encode the JSON string to bytes
        message_bytes = message_json.encode('utf-8')
        
        producer.send(KAFKA_TOPIC, value=message_bytes)
        producer.flush() # TODO: Do I want to flush here on every message of somewhere else?
        logger.info(f"Message sent to topic {KAFKA_TOPIC}")
    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {e}")
