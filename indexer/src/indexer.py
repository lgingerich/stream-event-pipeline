from loguru import logger
from typing import List, Dict
from web3 import Web3

def index_block(w3: Web3, block_number: int, contract_addresses: List[str], events: Dict):
    """
    Top level function to handle the indexing process for a given block
    """

    # Get all logs for the block without topic filtering first
    logs = get_logs(w3, block_number, contract_addresses)
    if not logs:
        logger.info(f"No logs found for block {block_number}")
        return [] # Return empty list if no logs

    # Decode logs
    # This will raise an error if decoding fails for a log with a known ABI
    processed_logs = decode_logs(w3, logs, events)

    return processed_logs

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

# TODO: To better handle topic hash collisions, match on contract address, not just topic hash
def decode_logs(w3: Web3, logs: List[dict], events: Dict) -> List[dict]:
    processed_logs = []
    
    for log in logs:
        # Extract common fields first
        base_log_data = {
            "block_hash": "0x" + log['blockHash'].hex() if isinstance(log['blockHash'], bytes) else log['blockHash'],
            "block_number": log['blockNumber'],
            "block_timestamp": int(log['blockTimestamp'], 16) if isinstance(log.get('blockTimestamp'), str) and log['blockTimestamp'].startswith('0x') else log.get('blockTimestamp'),
            "transaction_hash": "0x" + log['transactionHash'].hex() if isinstance(log['transactionHash'], bytes) else log['transactionHash'],
            "transaction_index": log['transactionIndex'],
            "log_index": log['logIndex'],
            "contract_address": log['address'],
            "data": "0x" + log['data'].hex() if isinstance(log['data'], bytes) else log['data'],
            "topics": ["0x" + topic.hex() if isinstance(topic, bytes) else topic for topic in log.get('topics', [])],
            "removed": log.get('removed', False)
        }

        final_log = None
        event_name = None
        contract_name = None
        decoded_data = None

        # Check if topics exist and are non-empty
        if log.get('topics') and len(log['topics']) > 0:
            topic_hash = log['topics'][0].hex() if isinstance(log['topics'][0], bytes) else log['topics'][0]
            
            # Check if we know this event ABI
            if topic_hash in events.get("by_topic", {}):
                event_info = events["by_topic"][topic_hash]
                
                # Handle case where event_info is a list (multiple events with same signature)
                if isinstance(event_info, list):
                    # TODO: Decide how to handle ambiguous events - using first for now
                    logger.warning(f"Multiple event ABIs found for topic {topic_hash}. Using the first one.")
                    event_info = event_info[0]
                
                try:
                    # Decode the individual log - this will raise error if decoding fails
                    event_name, contract_name, decoded_data = decode_single_log(w3, log, event_info)
                    final_log = {**base_log_data, "event": event_name, "contract": contract_name, "decoded_data": decoded_data}
                except Exception as e:
                    logger.error(f"Failed to decode log for event {event_info.get('name', 'unknown')} (Topic: {topic_hash}) in block {base_log_data['block_number']} Tx: {base_log_data['transaction_hash']}: {e}")
                    raise e 
            else:
                # ABI not found for this topic
                logger.debug(f"ABI not found for topic {topic_hash} in block {base_log_data['block_number']}. Storing raw log.")
                final_log = {**base_log_data, "event": None, "contract": None, "decoded_data": None}
        else:
            # Log has no topics or empty topics list
            logger.debug(f"Log in block {base_log_data['block_number']} has no topics. Storing raw log.")
            final_log = {**base_log_data, "event": None, "contract": None, "decoded_data": None}

        if final_log:
            processed_logs.append(final_log)
    
    return processed_logs

def decode_single_log(w3: Web3, log: dict, event_info: dict) -> tuple[str, str, dict] | None:
    """
    Decode a single log entry using its event info.
    Returns tuple(event_name, contract_name, decoded_args) or raises error.
    """
    # Create a contract with just this event
    contract = w3.eth.contract(abi=[{
        "type": "event",
        "name": event_info["name"],
        "inputs": event_info["inputs"],
        "anonymous": False
    }])

    # Process the log - let errors propagate
    event_obj = getattr(contract.events, event_info["name"])
    decoded_event = event_obj().process_log(log)

    return event_info["name"], event_info["contract"], dict(decoded_event["args"])
