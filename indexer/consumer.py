from kafka import KafkaConsumer
from loguru import logger
from typing import Dict, Optional
import json

class Consumer:
    def __init__(self, *topics, **configs):
        try:
            self.consumer = KafkaConsumer(
                *topics,
                **configs
            )
            logger.info("Consumer successfully connected to Kafka.")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise e

    def consume_message(self) -> Optional[Dict]:
        try:
            # Poll for messages
            poll_data = self.consumer.poll(timeout_ms=1000)
            
            # Check if we received any messages
            if not poll_data:
                return None
            
            # Process received messages
            for tp, messages in poll_data.items():
                for message in messages:
                    # Log and return the first message's value
                    if message and message.value:
                        # Deserialize the message
                        try:
                            decoded_message = json.loads(message.value.decode('utf-8'))
                            logger.info(f"Consumed message: {decoded_message}")
                            return decoded_message
                        except json.JSONDecodeError:
                            logger.error(f"Failed to decode message: {message.value}")
                    
            return None
        except Exception as e:
            logger.error(f"Failed to consume message: {e}")
            return None

    def close(self):
        self.consumer.close()
        logger.info("Successfully closed Kafka consumer.")
