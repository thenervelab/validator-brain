"""
RabbitMQ processor for listening to substrate chain user storage requests.

This processor:
1. Connects to the substrate chain
2. Listens for ipfsPallet.userStorageRequests storage changes
3. Parses the storage request data
4. Sends individual request items to RabbitMQ queue
"""

import asyncio
import json
import logging
import os
import sys
from typing import List, Tuple, Dict, Any

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv
from substrateinterface import SubstrateInterface

from app.utils.config import NODE_URL
from app.services.substrate_client import substrate_client

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Use pinning-specific node URL for reliable connection
PINNING_NODE_URL = os.getenv('PINNING_NODE_URL', NODE_URL)  # Fallback to NODE_URL if not set

class PinningRequestProcessor:
    """Processor for fetching and queuing user storage requests."""
    
    def __init__(self):
        """Initialize the processor."""
        self.substrate = None
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.queue_name = 'pinning_request'
        
    def connect_substrate(self):
        """Connect to the substrate chain."""
        logger.info(f"Connecting to substrate at {PINNING_NODE_URL}")
        self.substrate = SubstrateInterface(url=PINNING_NODE_URL)
        logger.info("Connected to substrate")
        
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ and declare the queue."""
        rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        
        self.rabbitmq_connection = await aio_pika.connect_robust(rabbitmq_url)
        self.rabbitmq_channel = await self.rabbitmq_connection.channel()
        
        # Declare the queue
        await self.rabbitmq_channel.declare_queue(
            self.queue_name,
            durable=True
        )
        
        logger.info(f"Connected to RabbitMQ and declared queue '{self.queue_name}'")
    
    def parse_storage_request_data(self, storage_data: List[Tuple[Any, Any]]) -> List[Dict[str, Any]]:
        """
        Parse the raw storage data from substrate into structured format.
        
        Args:
            storage_data: Raw data from substrate storage query
            
        Returns:
            List of parsed storage request dictionaries
        """
        parsed_requests = []
        
        for item in storage_data:
            if len(item) == 2:
                # Extract key and value
                key_data = item[0]
                value_data = item[1]
                
                # The key contains [owner, request_hash]
                if isinstance(key_data, list) and len(key_data) >= 2:
                    owner = str(key_data[0])
                    request_hash = str(key_data[1])
                    
                    # Parse the value data
                    if isinstance(value_data, dict):
                        # Handle both camelCase and snake_case field names
                        request = {
                            'owner': owner,
                            'request_hash': request_hash,
                            'file_hash': str(value_data.get('file_hash', value_data.get('fileHash', ''))),
                            'file_name': str(value_data.get('file_name', value_data.get('fileName', ''))),
                            'total_replicas': int(value_data.get('total_replicas', value_data.get('totalReplicas', 0))),
                            'is_assigned': bool(value_data.get('is_assigned', value_data.get('isAssigned', False))),
                            'selected_validator': str(value_data.get('selected_validator', value_data.get('selectedValidator', ''))),
                            'created_at': int(value_data.get('created_at', value_data.get('createdAt', 0))),
                            'last_charged_at': int(value_data.get('last_charged_at', value_data.get('lastChargedAt', 0))),
                            'miner_ids': value_data.get('miner_ids', value_data.get('minerIds', [])),
                            'timestamp': asyncio.get_event_loop().time()
                        }

                        parsed_requests.append(request)
                        logger.debug(f"Parsed storage request: {owner} -> {request_hash}")
                    else:
                        logger.warning(f"Invalid value format for key {key_data}")
                else:
                    logger.warning(f"Invalid key format: {key_data}")

        return parsed_requests

    async def check_user_has_credits(self, account_id: str) -> bool:
        """
        Check if user has non-zero account balance.

        Args:
            account_id: The account ID to check

        Returns:
            True if user has credits (balance > 0), False otherwise
        """
        try:
            balance = await substrate_client.check_user_balance(account_id)
            return balance > 0
        except Exception as e:
            logger.error(f"Error checking balance for {account_id}: {e}")
            return False

    async def fetch_and_queue_requests(self, run_once: bool = True):
        """
        Fetch storage requests from substrate and queue them.

        Args:
            run_once: If True, fetch once and exit. If False, run continuously.
        """
        while True:
            try:
                # Query all user storage requests
                result = self.substrate.query_map(
                    module='IpfsPallet',
                    storage_function='UserStorageRequests'
                )

                # First pass: collect all unique users and raw data
                raw_storage_data = []
                unique_users = set()
                total_entries = 0
                null_entries = 0
                
                for key, value in result:
                    total_entries += 1
                    
                    # Handle scale_info wrapped keys and values
                    try:
                        # Extract account and request_hash from key
                        if hasattr(key, '__iter__') and len(key) >= 2:
                            # Handle scale_info wrapped account
                            account = key[0]
                            if hasattr(account, 'value'):
                                account = str(account.value)
                            else:
                                account = str(account)

                            # Add user to set for batch credit checking
                            unique_users.add(account)

                            # Handle scale_info wrapped request_hash
                            request_hash = key[1]
                            if hasattr(request_hash, 'value'):
                                request_hash = str(request_hash.value)
                            else:
                                request_hash = str(request_hash)
                            
                            # Handle scale_info wrapped value
                            if value is not None:
                                actual_value = value
                                if hasattr(value, 'value'):
                                    actual_value = value.value

                                if actual_value is not None:
                                    raw_storage_data.append({
                                        'account': account,
                                        'request_hash': request_hash,
                                        'value': actual_value
                                    })
                                    logger.debug(f"Collected storage request: {account} -> {request_hash[:16]}...")
                                else:
                                    null_entries += 1
                                    logger.debug(f"Found null value for {account} -> {request_hash}")
                            else:
                                null_entries += 1
                                logger.debug(f"Found null entry for {account} -> {request_hash}")
                        else:
                            logger.warning(f"Invalid key format: {key}")
                            
                    except Exception as e:
                        logger.error(f"Error parsing entry {key}: {e}")
                        continue

                logger.info(f"🔍 DEBUG: Found {total_entries} total entries, {null_entries} null values")
                logger.info(f"🔍 DEBUG: Collected {len(raw_storage_data)} storage requests from {len(unique_users)} unique users")
                
                # ===== STORAGE REQUEST TRACING - STEP 1: FETCHED FROM SUBSTRATE =====
                if raw_storage_data:
                    logger.info(f"📥 SUBSTRATE_FETCH: Found {len(raw_storage_data)} raw storage requests from blockchain")
                    for i, data in enumerate(raw_storage_data[:5]):  # Log first 5 in detail
                        account = data['account'][:16] + "..."
                        request_hash = data['request_hash'][:16] + "..."
                        logger.info(f"📥 SUBSTRATE_FETCH[{i+1}]: account={account} request_hash={request_hash}")
                        
                        # Extract and log CID if available
                        try:
                            value = data['value']
                            if hasattr(value, 'get'):
                                file_hash = value.get('file_hash') or value.get('fileHash')
                                if file_hash:
                                    # Convert hex to string for logging
                                    if isinstance(file_hash, str) and file_hash.startswith('0x'):
                                        try:
                                            cid = bytes.fromhex(file_hash[2:]).decode('utf-8')
                                            logger.info(f"📥 SUBSTRATE_FETCH[{i+1}]: account={account} CID={cid[:20]}...")
                                        except:
                                            logger.info(f"📥 SUBSTRATE_FETCH[{i+1}]: account={account} file_hash_hex={file_hash[:20]}...")
                        except Exception as e:
                            logger.debug(f"Could not extract CID from raw data: {e}")
                    
                    if len(raw_storage_data) > 5:
                        logger.info(f"📥 SUBSTRATE_FETCH: ... and {len(raw_storage_data) - 5} more storage requests fetched")
                else:
                    logger.info(f"📥 SUBSTRATE_FETCH: No storage requests found on blockchain")

                # Fetch all user credits in parallel
                logger.info(f"Fetching credits for {len(unique_users)} users in parallel...")
                user_balances = await substrate_client.check_multiple_user_balances(list(unique_users))

                # Filter requests based on user credits
                storage_data = []
                zero_credit_users = set()

                for request_data in raw_storage_data:
                    account = request_data['account']
                    balance = user_balances.get(account, 0)
                    
                    if balance > 0:
                        # User has credits, include the request
                        storage_data.append([
                            [account, request_data['request_hash']], 
                            request_data['value']
                        ])
                    else:
                        # User has no credits, filter out
                        zero_credit_users.add(account)

                filtered_by_credits = len(zero_credit_users)
                if filtered_by_credits > 0:
                    logger.info(f"🔍 DEBUG: Filtered out storage requests from {filtered_by_credits} users with zero credits")
                    logger.info(f"🔍 DEBUG: Zero credit users: {list(zero_credit_users)[:5]}...")
                
                logger.info(f"🔍 DEBUG: Final result: {len(storage_data)} storage requests after credit filtering")
                
                # ===== STORAGE REQUEST TRACING - STEP 2: AFTER CREDIT FILTERING =====
                if storage_data:
                    logger.info(f"💳 CREDIT_FILTER: {len(storage_data)} storage requests remain after credit filtering")
                    for i, data in enumerate(storage_data[:3]):  # Log first 3 in detail
                        key_data = data[0]
                        value_data = data[1]
                        if len(key_data) >= 2:
                            account = str(key_data[0])[:16] + "..."
                            request_hash = str(key_data[1])[:16] + "..."
                            logger.info(f"💳 CREDIT_FILTER[{i+1}]: account={account} request_hash={request_hash} - User has credits, proceeding")
                    if len(storage_data) > 3:
                        logger.info(f"💳 CREDIT_FILTER: ... and {len(storage_data) - 3} more requests with valid credits")
                else:
                    logger.info(f"💳 CREDIT_FILTER: No storage requests remain after credit filtering")

                # Parse the data
                parsed_requests = self.parse_storage_request_data(storage_data)
                
                # ===== STORAGE REQUEST TRACING - STEP 3: PARSED REQUESTS READY FOR QUEUE =====
                if parsed_requests:
                    logger.info(f"🔄 PARSE_COMPLETE: Successfully parsed {len(parsed_requests)} storage requests for queue publishing")
                    for i, req in enumerate(parsed_requests[:3]):  # Log first 3 in detail
                        account = req['owner'][:16] + "..."
                        request_hash = req['request_hash'][:16] + "..."
                        file_hash_hex = req.get('file_hash', 'N/A')[:20] + "..."
                        
                        # Convert hex file_hash to CID for logging
                        cid = "N/A"
                        try:
                            file_hash = req.get('file_hash', '')
                            if file_hash and file_hash.startswith('0x'):
                                cid = bytes.fromhex(file_hash[2:]).decode('utf-8')[:20] + "..."
                            elif file_hash and not file_hash.startswith('0x'):
                                cid = bytes.fromhex(file_hash).decode('utf-8')[:20] + "..."
                        except:
                            cid = f"hex_{file_hash_hex}"
                        
                        is_assigned = req.get('is_assigned', False)
                        logger.info(f"🔄 PARSE_COMPLETE[{i+1}]: account={account} CID={cid} request_hash={request_hash} assigned={is_assigned}")
                    
                    if len(parsed_requests) > 3:
                        logger.info(f"🔄 PARSE_COMPLETE: ... and {len(parsed_requests) - 3} more requests ready for queue")
                else:
                    logger.info(f"🔄 PARSE_COMPLETE: No requests successfully parsed from storage data")

                # Process ALL storage requests (assigned + unassigned)
                assigned_count = sum(1 for req in parsed_requests if req.get('is_assigned', False))
                logger.info(f"🔍 Found {len(parsed_requests)} storage requests ({assigned_count} assigned, {len(parsed_requests) - assigned_count} unassigned)")
                
                # ===== STORAGE REQUEST TRACING - STEP 4: PUBLISHING TO QUEUE =====
                if parsed_requests:
                    logger.info(f"📤 QUEUE_PUBLISH: Starting to publish {len(parsed_requests)} storage requests to 'pinning_request' queue")
                    
                    # Log a few requests being published for tracing
                    for i, req in enumerate(parsed_requests[:3]):
                        account = req['owner'][:16] + "..."
                        request_hash = req['request_hash'][:16] + "..."
                        try:
                            file_hash = req.get('file_hash', '')
                            if file_hash and file_hash.startswith('0x'):
                                cid = bytes.fromhex(file_hash[2:]).decode('utf-8')[:20] + "..."
                            elif file_hash and not file_hash.startswith('0x'):
                                cid = bytes.fromhex(file_hash).decode('utf-8')[:20] + "..."
                            else:
                                cid = "N/A"
                        except:
                            cid = "parse_error"
                        logger.info(f"📤 QUEUE_PUBLISH[{i+1}]: Publishing account={account} CID={cid} request_hash={request_hash}")
                    
                    await self._publish_requests_parallel(parsed_requests)
                    logger.info(f"📤 QUEUE_PUBLISH: Successfully published {len(parsed_requests)} storage requests to queue")
                
                logger.info(f"Successfully processed {len(parsed_requests)} storage requests")
                
                if run_once:
                    break
                    
                # Wait before next fetch
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error fetching/queuing storage requests: {e}")
                if run_once:
                    raise
                await asyncio.sleep(10)  # Wait before retry
    
    async def _publish_requests_parallel(self, requests: List[Dict[str, Any]]) -> None:
        semaphore = asyncio.Semaphore(50)
        
        async def _publish_single_request(request: Dict[str, Any]) -> None:
            async with semaphore:
                try:
                    message_body = json.dumps(request).encode()
                    await self.rabbitmq_channel.default_exchange.publish(
                        aio_pika.Message(
                            body=message_body,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                        ),
                        routing_key=self.queue_name
                    )
                    # Log successful individual publish for key requests
                    account = request.get('owner', 'unknown')[:16] + "..."
                    request_hash = request.get('request_hash', 'unknown')[:16] + "..."
                    logger.debug(f"📤 QUEUE_PUBLISH_SUCCESS: account={account} request_hash={request_hash}")
                except Exception as e:
                    account = request.get('owner', 'unknown')[:16] + "..."
                    request_hash = request.get('request_hash', 'unknown')[:16] + "..."
                    logger.error(f"📤 QUEUE_PUBLISH_FAILED: account={account} request_hash={request_hash} error={e}")
                    raise
        
        tasks = [_publish_single_request(request) for request in requests]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = sum(1 for r in results if not isinstance(r, Exception))
        failed = len(results) - successful
        
        if failed > 0:
            logger.warning(f"Parallel publishing: {successful} succeeded, {failed} failed")
        else:
            logger.info(f"Parallel publishing: {successful}/{len(requests)} requests published successfully")
    
    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")
        
        if self.substrate:
            self.substrate.close()
            logger.info("Closed substrate connection")


async def main():
    """Main entry point."""
    processor = PinningRequestProcessor()
    
    try:
        # Connect to services
        processor.connect_substrate()
        await processor.connect_rabbitmq()
        
        # Fetch and queue requests
        await processor.fetch_and_queue_requests(run_once=True)
        
    except Exception as e:
        logger.error(f"Error in processor: {e}")
        raise
    finally:
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main())