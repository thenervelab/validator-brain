Goal:

- substrate chain
- ipfs network

what we need in the database :
- representation of a miner ( each pinned file he handle )
- list of all the cid in the network 
- list of all users profiles 





- keep a db of a mapping between cid and miners profiles and user profile
- listen to miner profile change at each block and update database accordingly

- we need to assign an IPFS file with the correct replicas to available miners for that we need:
    1) fetch the request from the chain and mark it as in progress
    2) request miner locks
    3) download the pining request from the ipfs network and check each file size
    4) find available miners based on available space
    5) shuffle and distribute the replicas to the miners 
    6) pin the miners profile and update chain with new profiles
    5) update the chain with the new pinning request as resolved
    6) mark the pinning request as completed

- rebalance request ( when a miner come offline redistribute the file )
  1) check all the miners and discover the one offline
  2) get the cid that need to be rebalanced and fill it in the same process than pinning request 


- check if a miner is offline (ping test )
- check if miner is really storing the file (ipfs dag get with random block)


- handle unpin request (when a user want to unpin a file)
  1) fetch the request from the chain and mark it as in progress
  2) request miner locks
  3) remove the file from all miners profiles
  4) pin the miners profile and update chain with new profiles







Flow with Python (e.g., FastAPI):
- Chain Interaction: Use py-substrate-interface to listen for events or poll for new requests.
- API Endpoints (FastAPI): Define endpoints for your core logic.
- IPFS Interaction: Use ipfshttpclient (potentially wrapped in asyncio.to_thread if using FastAPI to avoid blocking the event loop for synchronous calls).
- Database: Use SQLAlchemy with an async driver like asyncpg or use its standard synchronous API within thread pool executors.
- Background Workers (Celery/Dramatiq):
- Miner health checks (ping, storage proof).
- Rebalancing logic.
- Processing steps of a pinning request that can be done asynchronously.

SQLAlchemy , asyncpg, postgres


Challange:

- check all the miners for each epoch
- check if a miner is offline
- check if a miner is storing the correct file
- rebalance the file across the miners
- handle both ipfs cid format


all that in a given timeframe of 2 hours, we actually have 600 nodes.







