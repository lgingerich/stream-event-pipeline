from dotenv import load_dotenv
from loguru import logger
import os
import sys
from consumer import Consumer
from db import DB

# TODO: I should only need to call this if not running in Docker
load_dotenv()

# Configure Loguru to show INFO and higher (INFO, WARNING, ERROR, CRITICAL)
logger.remove()
logger.add(sys.stderr, level="INFO")

consumer = None
db = None

def main():
    try:
        # Get Kafka config from environment variables
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        kafka_topic = os.getenv("KAFKA_TOPIC")
        if not kafka_bootstrap_servers or not kafka_topic:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC must be set")
        logger.info(f"Attempting to connect to Kafka brokers at: {kafka_bootstrap_servers}")
        logger.info(f"Target Kafka topic: {kafka_topic}")

        # Initialize Kafka consumer
        consumer = Consumer(
            kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers
        )

        # Get DB config from environment variables
        db_name = os.getenv("POSTGRES_DB")
        db_user = os.getenv("POSTGRES_USER")
        db_password = os.getenv("POSTGRES_PASSWORD")
        db_host = os.getenv("POSTGRES_HOST")
        db_port = os.getenv("POSTGRES_PORT")
        if not all([db_name, db_user, db_password, db_host, db_port]):
            raise ValueError("Database connection details (POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT) must be set")
        logger.info(f"Attempting to connect to database {db_name} at {db_host}:{db_port}")

        # Initialize DB connection and DB class instance
        db = DB(db_name, db_user, db_password, db_host, db_port)

        # Loop indefinitely to consume messages
        while True:
            # Consume messages
            consumed_messages = consumer.consume_messages()
            if consumed_messages:
                for message in consumed_messages:
                    logger.info(f"Successfully consumed message for tx: {message['transaction_hash']} log: {message['log_index']}")
                    # Insert consumed message into the database
                    try:
                        db.insert_data(message) 
                        logger.info(f"Inserted message for tx: {message.get('transaction_hash')} log: {message.get('log_index')}")
                    except Exception as e:
                        logger.error(f"Failed to insert consumed message into DB: {e}")
                        # TODO: Should first retry inserting the message, but for now just raise the error
                        raise e
            else:
                logger.info("No message consumed in this poll interval.")
                pass

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down gracefully.")

    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise e
    
    finally:
        # Close consumer
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed.")
        # Close DB connection
        if db:
            db.close()
            logger.info("Database connection closed.")
        logger.info("Shutdown complete, exiting main loop.")

if __name__ == "__main__":
    main()