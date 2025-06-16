"""
RabbitMQ processor for listening to substrate chain miner profiles.

This processor:
1. Connects to the substrate chain
2. Listens for ipfsPallet.minerProfile storage changes
3. Parses the miner profile data
4. Sends individual profile items to RabbitMQ queue
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

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinerProfileProcessor:
    """Processor that listens to substrate chain for miner profiles and sends them to RabbitMQ."""
    
    def __init__(self, substrate_url: str = None, rabbitmq_url: str = None):
        """
        Initialize the processor.
        
        Args:
            substrate_url: URL of the substrate node
            rabbitmq_url: URL of the RabbitMQ server
        """
        self.substrate_url = substrate_url or os.getenv('SUBSTRATE_URL', NODE_URL)
        self.rabbitmq_url = rabbitmq_url or os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'miner_profile'
        
        self.substrate = None
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.rabbitmq_queue = None
        
    def connect_substrate(self):
        """Connect to the substrate chain."""
        try:
            self.substrate = SubstrateInterface(
                url=self.substrate_url
            )
            logger.info(f"Connected to substrate at {self.substrate_url}")
        except Exception as e:
            logger.error(f"Failed to connect to substrate: {e}")
            raise
            
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ and declare the queue."""
        try:
            # Connect to RabbitMQ
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Declare the queue
            self.rabbitmq_queue = await self.rabbitmq_channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            logger.info(f"Connected to RabbitMQ and declared queue '{self.queue_name}'")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    def parse_miner_profile_data(self, storage_data: List[List[Any]]) -> List[Dict[str, str]]:
        """
        Parse the raw storage data from substrate into structured format.
        
        Args:
            storage_data: Raw data from substrate storage query
            
        Returns:
            List of parsed miner profile dictionaries
        """
        parsed_profiles = []
        
        for item in storage_data:
            if len(item) == 2:
                # Extract node_id and CID
                node_id = str(item[0][0]) if isinstance(item[0], list) and len(item[0]) > 0 else None
                cid = str(item[1]) if item[1] else None
                
                if node_id and cid:
                    profile = {
                        'node_id': node_id,
                        'cid': cid,
                        'timestamp': asyncio.get_event_loop().time()
                    }
                    parsed_profiles.append(profile)
                    logger.debug(f"Parsed profile: {profile}")
                else:
                    logger.warning(f"Invalid profile data: {item}")
        
        return parsed_profiles
    
    async def send_to_queue(self, profile: Dict[str, str]):
        """
        Send a single profile to the RabbitMQ queue.
        
        Args:
            profile: Profile dictionary to send
        """
        try:
            message_body = json.dumps(profile).encode()
            message = aio_pika.Message(
                body=message_body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )
            
            await self.rabbitmq_channel.default_exchange.publish(
                message,
                routing_key=self.queue_name
            )
            
            logger.info(f"Sent profile to queue: {profile['node_id']} -> {profile['cid']}")
        except Exception as e:
            logger.error(f"Failed to send message to queue: {e}")
            raise
    
    async def _send_profiles_parallel(self, profiles: List[Dict[str, str]]) -> None:
        """
        Send multiple profiles to RabbitMQ in parallel with rate limiting.
        
        Args:
            profiles: List of profile dictionaries to send
        """
        semaphore = asyncio.Semaphore(50)  # Limit concurrent publishing to 50
        
        async def _send_single_profile(profile: Dict[str, str]) -> None:
            """Send a single profile with semaphore protection."""
            async with semaphore:
                try:
                    message_body = json.dumps(profile).encode()
                    message = aio_pika.Message(
                        body=message_body,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    )
                    
                    await self.rabbitmq_channel.default_exchange.publish(
                        message,
                        routing_key=self.queue_name
                    )
                    logger.debug(f"✅ Published profile: {profile['node_id']} -> {profile['cid']}")
                except Exception as e:
                    logger.error(f"❌ Failed to publish profile {profile['node_id']}: {e}")
                    raise
        
        # Execute all publishing tasks in parallel
        tasks = [_send_single_profile(profile) for profile in profiles]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count successful vs failed publications
        successful = sum(1 for r in results if not isinstance(r, Exception))
        failed = len(results) - successful
        
        if failed > 0:
            logger.warning(f"⚠️ Parallel publishing: {successful} succeeded, {failed} failed")
        else:
            logger.info(f"✅ Parallel publishing: {successful}/{len(profiles)} profiles published successfully")
    
    async def fetch_and_process_profiles(self):
        """Fetch miner profiles from substrate and send them to RabbitMQ."""
        try:
            # Query the storage - get all key-value pairs
            result = self.substrate.query_map(
                module='IpfsPallet',
                storage_function='MinerProfile'
            )
            
            # Convert to list format matching the expected structure
            storage_data = []
            for key, value in result:
                # key is the node_id, value is the CID
                storage_data.append([[str(key.value)], str(value.value)])
            
            logger.info(f"Fetched {len(storage_data)} miner profiles from substrate")
            
            # Parse the profiles
            parsed_profiles = self.parse_miner_profile_data(storage_data)
            
            # Send all profiles to queue in parallel
            if parsed_profiles:
                await self._send_profiles_parallel(parsed_profiles)
                logger.info(f"📦 Batch processed {len(parsed_profiles)} profiles")
            
        except Exception as e:
            logger.error(f"Error fetching/processing profiles: {e}")
            raise
    
    async def run_once(self):
        """Run the processor once to fetch and send all current profiles."""
        try:
            # Connect to services
            self.connect_substrate()
            await self.connect_rabbitmq()
            
            # Fetch and process profiles
            await self.fetch_and_process_profiles()
            
        finally:
            # Clean up connections
            if self.rabbitmq_connection:
                await self.rabbitmq_connection.close()
                logger.info("Closed RabbitMQ connection")
    
    async def run_continuous(self, interval: int = 60):
        """
        Run the processor continuously, checking for updates at regular intervals.
        
        Args:
            interval: Seconds between checks
        """
        try:
            # Connect to services
            self.connect_substrate()
            await self.connect_rabbitmq()
            
            while True:
                try:
                    await self.fetch_and_process_profiles()
                    await asyncio.sleep(interval)
                except Exception as e:
                    logger.error(f"Error in processing loop: {e}")
                    await asyncio.sleep(interval)
                    
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        finally:
            # Clean up connections
            if self.rabbitmq_connection:
                await self.rabbitmq_connection.close()
                logger.info("Closed RabbitMQ connection")


async def main():
    """Main entry point for the processor."""
    processor = MinerProfileProcessor()
    
    # For now, just run once as requested
    await processor.run_once()
    
    # To run continuously, uncomment:
    # await processor.run_continuous(interval=60)


if __name__ == "__main__":
    asyncio.run(main()) 