#!/usr/bin/env python3

import asyncio
import sys
import os

# Add current directory to path
sys.path.append('.')

from rabbitmq.pinning_request_processor import PinningRequestProcessor

async def test_processor():
    processor = PinningRequestProcessor()
    try:
        print("Connecting to substrate...")
        processor.connect_substrate()
        print("Connecting to RabbitMQ...")
        await processor.connect_rabbitmq()
        print("Fetching and queuing requests...")
        await processor.fetch_and_queue_requests(run_once=True)
        print("Done.")
    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()
    finally:
        await processor.close()

if __name__ == "__main__":
    asyncio.run(test_processor())