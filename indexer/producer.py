import json
from kafka import KafkaProducer
from loguru import logger
from typing import List, Dict

class Producer:
    def __init__(self, **configs):
        try:
            self.producer = KafkaProducer(
                **configs
            )
            logger.info("Producer successfully connected to Kafka.")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise e

    def produce_message(self, topic: str, messages: List[Dict]):
        logger.info(f"Attempting to send {len(messages)} messages to topic '{topic}'.")
        try:
            # Split list of decoded logs and send individually
            for msg in messages:
                # TODO: Switch to better serialization strategy
                message_json = json.dumps(msg)
                message_bytes = message_json.encode('utf-8')

                self.producer.send(topic, value=message_bytes)
                tx_hash = msg.get("transaction_hash")
                log_index = msg.get("log_index")
                logger.info(f"Sent message for tx: {tx_hash} log: {log_index}")
            self.producer.flush() # Flush to guarantee delivery of all messages before returning
            logger.info(f"Successfully flushed {len(messages)} messages to topic '{topic}'.")
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            raise e

    def close(self):
        try:
            self.producer.close()
            logger.info("Successfully closed Kafka producer.")
        except Exception as e:
            logger.error(f"Failed to close Kafka producer: {e}")
            raise e
