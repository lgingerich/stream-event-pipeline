import json
from loguru import logger
from typing import List, Dict, Any
from web3 import Web3

from producer import produce_message

def index_block(w3: Web3, block_number: int, contract_addresses: List[str], events: Dict):
    """
    Top level function to handle the indexing process for a given block
    """

    # Get all logs for the block without topic filtering first
    logs = get_logs(w3, block_number, contract_addresses)
    if not logs:
        logger.info(f"No logs found for block {block_number}")
        return

    # Decode logs
    decoded_logs = decode_logs(w3, logs, events)

    # Transform logs to desired format
    transformed_logs = transform_logs(decoded_logs)
    print(transformed_logs)

    # Send cleaned logs to Kafka
    produce_message(transformed_logs)

def get_logs(w3: Web3, block_number: int, contract_addresses: List[str]) -> List[dict]:
    """
    Get all logs for a given block

    Do not filter by topics. If passing in a list of topics, it is easy to error on
    exceeding max length. Rather fetch all logs for the given contract addresses and
    then filter in the decode_logs function.
    """
    filter_params = {
        'fromBlock': block_number,
        'toBlock': block_number,
        'address': contract_addresses
    }
    try:
        logs = w3.eth.get_logs(filter_params)
        logger.info(f"Retrieved {len(logs)} logs for block {block_number}")
        return logs
    except Exception as e:
        logger.error(f"Error retrieving logs for block {block_number}: {e}")
        return []

# TODO: Match on contract address, not just topic hash
def decode_logs(w3: Web3, logs: List[dict], events: Dict) -> List[dict]:
    decoded_logs = []
    
    for log in logs:
        if not log.get('topics') or len(log['topics']) == 0:
            continue
            
        topic_hash = log['topics'][0].hex() if isinstance(log['topics'][0], bytes) else log['topics'][0]
        
        # Check if we know this event
        if topic_hash in events["by_topic"]:
            event_info = events["by_topic"][topic_hash]
            
            # Handle case where event_info is a list (multiple events with same signature)
            if isinstance(event_info, list):
                event_info = event_info[0]  # Use first matching event for now
            
            try:
                # Decode the individual log
                decoded_log = decode_single_log(w3, log, event_info)
                if decoded_log:
                    decoded_logs.append(decoded_log)
            except Exception as e:
                logger.error(f"Error decoding log: {e}")
    
    return decoded_logs

def decode_single_log(w3: Web3, log: dict, event_info: dict) -> dict | None:
    """
    Decode a single log entry using its event info
    """
    try:
        # Create a contract with just this event
        contract = w3.eth.contract(abi=[{
            "type": "event",
            "name": event_info["name"],
            "inputs": event_info["inputs"],
            "anonymous": False
        }])
        
        # Process the log
        event_obj = getattr(contract.events, event_info["name"])
        decoded_event = event_obj().process_log(log)
        
        result = {
            "block_hash": log['blockHash'].hex() if isinstance(log['blockHash'], bytes) else log['blockHash'],
            "block_number": log['blockNumber'],
            "block_timestamp": log['blockTimestamp'], 
            "transaction_hash": log['transactionHash'].hex() if isinstance(log['transactionHash'], bytes) else log['transactionHash'],
            "transaction_index": log['transactionIndex'],
            "log_index": log['logIndex'],
            "contract_address": log['address'],
            "data": log['data'].hex() if isinstance(log['data'], bytes) else log['data'],
            "topics": [topic.hex() if isinstance(topic, bytes) else topic for topic in log['topics']],
            "removed": log['removed'],
            "event": event_info["name"],
            "contract": event_info["contract"],
            "args": dict(decoded_event["args"])
        }
        
        return result
    except Exception as e:
        logger.error(f"Error in decode_single_log: {e}")
        return None

def transform_logs(logs: List[dict]) -> List[dict]:
    """
    Transform logs to desired format
    """
    result = []
    for log in logs:
        result.append({
            "block_hash": log['block_hash'],
            "block_number": log['block_number'],
            "block_timestamp": log['block_timestamp'], 
            "transaction_hash": log['transaction_hash'],
            "transaction_index": log['transaction_index'],
            "log_index": log['log_index'],
            "contract_address": log['contract_address'],
            "data": log['data'],
            "topics": log['topics'],
            "removed": log['removed'],
            "contract_name": log['contract'],
            "event": log['event'],
            "sender": log['args']['sender'],
            "recipient": log['args']['recipient'],
            "amount0": log['args']['amount0'],
            "amount1": log['args']['amount1'],
            "sqrt_price_x96": log['args']['sqrtPriceX96'],
            "liquidity": log['args']['liquidity'],
            "tick": log['args']['tick'],
        })
    return result
