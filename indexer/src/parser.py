import json
from loguru import logger
from typing import Dict, Any
from web3 import Web3
import os

def parse_abi(abi_directory: str) -> Dict[str, Dict[str, Any]]:
    """
    Process all JSON files in a directory and extract event signatures and topics.
    Returns a dictionary mapping event topic hashes to event details.
    """
    result = {
        "by_topic": {}
    }
    
    # List all JSON files in the directory
    for filename in os.listdir(abi_directory):
        if not filename.endswith('.json'):
            continue
            
        file_path = os.path.join(abi_directory, filename)
        
        try:
            # Open and parse the JSON file
            with open(file_path) as f:
                abi_data = json.load(f)
            
            # Normalize ABI format
            if isinstance(abi_data, dict):
                if "type" in abi_data and abi_data["type"] == "event":
                    # Single event
                    abi = [abi_data]
                elif "abi" in abi_data:
                    # Contract with ABI field
                    abi = abi_data["abi"]
                else:
                    # Skip files with unsupported format
                    print(f"Skipping {filename}: Unsupported ABI format")
                    continue
            elif isinstance(abi_data, list):
                # List of ABI elements
                abi = abi_data
            else:
                # Skip files with unsupported format
                print(f"Skipping {filename}: Unsupported ABI format")
                continue
            
            # Extract events from this ABI
            for item in abi:
                if item.get("type") != "event":
                    continue
                    
                name = item["name"]
                param_types = ",".join([inp["type"] for inp in item["inputs"]])
                signature = f"{name}({param_types})"
                topic = Web3.keccak(text=signature).hex()
                
                # Store event info with contract name (from filename)
                contract_name = os.path.splitext(filename)[0]
                event_key = f"{contract_name}.{name}"
                
                event_info = {
                    "contract": contract_name,
                    "name": name,
                    "signature": signature,
                    "topic": topic,
                    "inputs": item["inputs"]
                }
                
                # For topic lookup, note that multiple contracts might have the same event signature
                # If there's a collision, we'll store a list of all matching events
                if topic in result["by_topic"]:
                    # If this is the first collision, convert to list
                    if not isinstance(result["by_topic"][topic], list):
                        result["by_topic"][topic] = [result["by_topic"][topic]]
                    # Add this event to the list
                    result["by_topic"][topic].append(event_info)
                else:
                    # No collision, just store the event
                    result["by_topic"][topic] = event_info
            
            logger.info(f"Processed {filename}: found {len([i for i in abi if i.get('type') == 'event'])} events")
            
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
    
    return result
