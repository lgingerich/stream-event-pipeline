from dotenv import load_dotenv
import json
from loguru import logger
import os
from typing import Tuple, Dict, List, Any, cast
from web3 import Web3
from web3.types import FilterParams, LogReceipt
import psycopg2

# Load environment variables
load_dotenv()

def abi_loader() -> Tuple[str, str, str]:
    with open("abi/uniswap-v3-router-swap.json") as f:
        abi_data = json.load(f)
    
    # Handle the case where the file contains a single event definition
    if isinstance(abi_data, dict) and "type" in abi_data and abi_data["type"] == "event":
        # For a single event, we can process it directly
        event = cast(Dict[str, Any], abi_data)
        name = event['name']
        inputs = event['inputs']
        param_types = ",".join([inp['type'] for inp in inputs])
        signature = f"{name}({param_types})"
        topic = Web3.keccak(text=signature).hex()
        
        print(f"Event Name: {name}")
        print(f"Signature: {signature}")
        print(f"Topic: {topic}")
        return name, signature, topic

    # Handle other ABI formats
    if isinstance(abi_data, dict) and 'abi' in abi_data:
        abi = abi_data['abi']
    elif isinstance(abi_data, list):
        abi = abi_data
    else:
        raise ValueError("Error: Unsupported ABI format")

    # Find the first event in the ABI
    event_abis = [item for item in abi if item.get("type") == "event"]
    
    if not event_abis:
        raise ValueError("No events found in the ABI")
    
    # Use the first event
    event = cast(Dict[str, Any], event_abis[0])
    name = event['name']
    inputs = event['inputs']
    param_types = ",".join([inp['type'] for inp in inputs])
    signature = f"{name}({param_types})"
    topic = Web3.keccak(text=signature).hex()
    
    print(f"Event Name: {name}")
    print(f"Signature: {signature}")
    print(f"Topic: {topic}")
    
    return name, signature, topic


def decode_logs(logs: List[LogReceipt], event_name: str, w3: Web3) -> List[Dict[str, Any]]:
    # Load ABI to decode events
    with open("abi/uniswap-v3-router-swap.json") as f:
        abi_data = json.load(f)
    
    # Normalize ABI structure
    if isinstance(abi_data, dict) and 'abi' in abi_data:
        abi = abi_data['abi']
    elif isinstance(abi_data, list):
        abi = abi_data
    else:
        abi = [abi_data]  # single event case

    contract = w3.eth.contract(abi=abi)

    decoded_logs = []
    for log in logs:
        try:
            event_class = getattr(contract.events, event_name)
            decoded_event = event_class().process_log(log)
            print("Decoded Event Args:")
            print(decoded_event['args'])
            decoded_logs.append(decoded_event['args'])
        except Exception as e:
            print(f"Failed to decode log: {e}")
    return decoded_logs

def main():
    # Get RPC URL from environment variable
    rpc_url = os.getenv("RPC_URL")
    if not rpc_url:
        raise ValueError("RPC_URL is not set")
    
    # Initialize Web3
    w3 = Web3(Web3.HTTPProvider(rpc_url))

    # Parse ABI
    event_name, signature, topic = abi_loader()

    contract_address = Web3.to_checksum_address("0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640") # Uniswap v3 USDC / ETH 0.05% pool contract address
    
    filter_params: FilterParams = {
        'fromBlock': 22327400,
        'toBlock': 22327401,
        'address': contract_address,
        'topics': ['0x' + topic]
    }

    # Get raw logs
    logs = w3.eth.get_logs(filter_params)

    # Decode logs
    # TODO: Check if ABI is available for decoding
    decoded_logs = [] # Initialize to empty list
    if logs: # Only decode if there are logs
        decoded_logs = decode_logs(logs, event_name, w3)

    parsed_logs = []
    for log in logs:
        entry = {
            
            "block_hash": log['blockHash'].hex() if isinstance(log['blockHash'], bytes) else log['blockHash'],
            "block_number": log['blockNumber'],
            "block_timestamp": log['blockTimestamp'], 
            "transaction_hash": log['transactionHash'].hex() if isinstance(log['transactionHash'], bytes) else log['transactionHash'],
            "transaction_index": log['transactionIndex'],
            "log_index": log['logIndex'],
            "contract_address": log['address'],
            "data": log['data'],
            "topics": [topic.hex() if isinstance(topic, bytes) else topic for topic in log['topics']],
            "removed": log['removed'],
        }
        parsed_logs.append(entry)

    print("Parsed Logs:")
    print(parsed_logs)

if __name__ == "__main__":
    main()
