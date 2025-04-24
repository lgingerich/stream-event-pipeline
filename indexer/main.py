from dotenv import load_dotenv
from loguru import logger
import os
from web3 import Web3

from indexer import index_block
from parser import parse_abi
from producer import Producer
from consumer import Consumer

# TODO: I should only need to call this if not running in Docker
load_dotenv()

def main():
    # Get RPC URL from environment variable
    rpc_url = os.getenv("RPC_URL")
    if not rpc_url:
        raise ValueError("RPC_URL is not set")
    logger.info(f"RPC_URL: {rpc_url}")

    # Initialize Web3 provider
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise ValueError("Failed to connect to RPC")

    # Get start and end block from environment variables (optional params)
    start_block = os.getenv("START_BLOCK")
    if start_block:
        logger.info(f"START_BLOCK: {start_block}")
        start_block = int(start_block)
    else:
        logger.info("START_BLOCK not set, indexing from block 0")
        start_block = 0
    end_block = os.getenv("END_BLOCK")
    if end_block:
        logger.info(f"END_BLOCK: {end_block}")
        end_block = int(end_block)
    else:
        logger.info("END_BLOCK not set, indexing continues indefinitely")
        end_block = None

    # Get Kafka config from environment variables
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic = os.getenv("KAFKA_TOPIC")
    if not kafka_bootstrap_servers or not kafka_topic:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC must be set")
    logger.info(f"Attempting to connect to Kafka brokers at: {kafka_bootstrap_servers}")
    logger.info(f"Target Kafka topic: {kafka_topic}")

    # Initialize Kafka producer
    producer = Producer(
        bootstrap_servers=kafka_bootstrap_servers,
        retries=5,
        retry_backoff_ms=1000
    )

    # Initialize Kafka consumer
    consumer = Consumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers
        # group_id="blockchain-indexer",
        # auto_offset_reset="earliest"
    )

    # Get event signatures and topics
    events = parse_abi("./abi") # Pass in directory of all ABIs
    if not events:
        raise ValueError("Unable to parse ABIs, or no events found")

    # Initialize variables for indexing loop
    running = True
    block_to_process = start_block

    # TODO: How to better handle multiple contract addresses?
    contract_addresses = [Web3.to_checksum_address("0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640")] # Uniswap v3 USDC / ETH 0.05% pool contract address

    # Indexing loop
    while running:
        try:
            # Check if we've reached the end block, and if so, stop indexing
            if end_block is not None and block_to_process > end_block:
                logger.info("Reached end block, stopping indexing")
                running = False
                break
            
            # Index block and return cleaned logs
            logs = index_block(w3, block_to_process, contract_addresses, events)
            
            # Send cleaned logs to Kafka
            producer.produce_message(kafka_topic, logs)

            # Consume messages
            consumed_message = consumer.consume_message()
            if consumed_message:
                logger.info(f"Successfully consumed message: {consumed_message}")
            else:
                logger.info("No message consumed in this poll interval.")

            # Increment block to process
            block_to_process += 1
                
        except Exception as e:
            logger.error(f"Error during indexing: {e}")
            running = False
            break

    # Close Kafka producer and consumer
    producer.close()
    consumer.close()

if __name__ == "__main__":
    main()
