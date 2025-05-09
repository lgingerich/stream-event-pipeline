from dotenv import load_dotenv
from loguru import logger
import os
import sys
import time
from web3 import Web3

from indexer import index_block
from parser import parse_abi
from producer import Producer

load_dotenv()

# Configure Loguru to show INFO and higher (INFO, WARNING, ERROR, CRITICAL)
logger.remove()
logger.add(sys.stderr, level="INFO")

def main():
    producer = None

    try:
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

        # Get RPC URL from environment variable
        rpc_url = os.getenv("RPC_URL")
        if not rpc_url:
            raise ValueError("RPC_URL is not set")
        logger.info(f"RPC_URL: {rpc_url}")

        # Initialize Web3 provider
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        if not w3.is_connected():
            raise ValueError("Failed to connect to RPC")

        # Get start and end block from environment variables
        # START_BLOCK can be:
        # - An integer block number (e.g., 12345670)
        # - If not set, defaults to 0
        # END_BLOCK can be:
        # - An integer block number (e.g., 12345678)
        # - If not set, indexing continues indefinitely
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

        # Get event signatures and topics
        # This function will parse all ABIs in the ./abi directory
        # and return a list of events with their name, signature, topic, and inputs
        # TODO: Would be better if moved to a totally separate process and loaded in 
        # as a dependency for the indexer
        events = parse_abi("./abi")
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

                # Check if we are within 100 blocks of chain tip. This acts as a buffer
                # to avoid needing reorg detection.
                # If we hit this buffer, sleep for 12 seconds to avoid catching up.
                latest_block = w3.eth.block_number
                if block_to_process > latest_block - 100:
                    logger.info(f"Block {block_to_process} is within 100 blocks of chain tip {latest_block}. Sleeping for 12 seconds.")
                    time.sleep(12)
                    continue
                
                # Index block and return cleaned logs
                logger.info(f"Indexing block {block_to_process}")
                logs = index_block(w3, block_to_process, contract_addresses, events)
                
                # Send cleaned logs to Kafka
                if logs: # Only produce if there are logs
                    producer.produce_message(kafka_topic, logs)
                    logger.info(f"Sent {len(logs)} logs from block {block_to_process} to Kafka")
                else:
                    logger.info(f"No relevant logs found in block {block_to_process}")

                # Increment block to process
                block_to_process += 1
                logger.info(f"Finished indexing block {block_to_process}")
            except Exception as e:
                logger.error(f"Error during indexing block {block_to_process}: {e}")

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down gracefully.")

    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise e
    
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("Shutdown complete, exiting main loop.")

if __name__ == "__main__":
    main()
