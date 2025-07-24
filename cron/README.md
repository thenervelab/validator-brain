# Chain Data Pinner Cron Script

This standalone cron script fetches user profiles and storage requests from the Substrate blockchain and pins them to a local IPFS node.

## Overview

The script runs independently of the main validator orchestrator and performs the following tasks:

1. **Fetches User Profiles**: Queries `IpfsPallet::UserProfile` storage to get all user profile CIDs
2. **Processes Profile Contents**: Downloads each user profile JSON and extracts individual file CIDs from the `file_hash` fields
3. **Fetches Storage Requests**: Queries `IpfsPallet::UserStorageRequests` storage to get all file CIDs from storage requests
4. **Pins to IPFS**: Pins all discovered CIDs (profile CIDs + individual file CIDs + storage request CIDs) to the local IPFS node
5. **Avoids Duplicates**: Checks already pinned CIDs and skips them to avoid redundant work

## Installation

1. Create and activate a virtual environment:
```bash
cd cron/
python3 -m venv .venv
source .venv/bin/activate
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Ensure you have a local IPFS node running:
```bash
ipfs daemon
```

## Configuration

The script uses environment variables for configuration:

- `NODE_URL`: Substrate RPC endpoint (default: `wss://rpc.hippius.network`)
- `IPFS_API_URL`: Local IPFS API endpoint (default: `http://127.0.0.1:5001`)
- `IPFS_GATEWAY_URL`: IPFS gateway for fetching profile contents (default: `https://get.hippius.network`)

## Usage

### Manual Execution

Run the script once manually:

```bash
# Activate virtual environment first
source .venv/bin/activate
python chain_data_pinner.py
```

### Cron Job Setup

To run every 3 minutes, add this to your crontab:

```bash
# Edit crontab
crontab -e

# Add this line to run every 3 minutes (sources virtual environment)
*/3 * * * * cd /path/to/ipfs-service-validator/cron && source .venv/bin/activate && python chain_data_pinner.py >> /var/log/chain-pinner.log 2>&1
```

Example with full paths:
```bash
*/3 * * * * cd /home/ubuntu/ipfs-service-validator/cron && source .venv/bin/activate && python chain_data_pinner.py >> /var/log/chain-pinner.log 2>&1
```

## Features

- **Independent Operation**: Runs separately from the main validator
- **Efficient**: Only pins new CIDs that aren't already pinned locally
- **Parallel Processing**: 10 concurrent profile downloads + 50 concurrent CID pins with timeouts
- **Robust Error Handling**: Continues processing even if individual CIDs fail
- **Comprehensive Logging**: Detailed logs for monitoring and debugging
- **Configurable**: Environment variable configuration
- **High Performance**: Async parallel processing for maximum throughput

## Output

The script provides detailed logging including:

- Number of user profiles found
- Number of storage requests found  
- CIDs already pinned (skipped)
- New CIDs successfully pinned
- Any failures with error details
- Total processing time and summary statistics

Example output:
```
2024-01-20 10:00:01 - INFO - Starting chain data pinner cron job
2024-01-20 10:00:01 - INFO - Connected to substrate at wss://rpc.hippius.network
2024-01-20 10:00:02 - INFO - Fetched 150 user profiles
2024-01-20 10:00:03 - INFO - Fetched 1200 user storage requests
2024-01-20 10:00:03 - INFO - Found 150 user profiles and 1200 storage requests from chain
2024-01-20 10:00:04 - INFO - Fetching user profile contents to extract file CIDs...
2024-01-20 10:00:05 - INFO - Profile bafkrei... for 5EvT2c...: extracted 8 file CIDs
2024-01-20 10:00:05 - INFO - Profile bafkrei... for 5HoreG...: extracted 12 file CIDs
2024-01-20 10:00:12 - INFO - Extracted 1500 file CIDs from 150 user profiles
2024-01-20 10:00:15 - INFO - Found 2850 total new CIDs to pin
2024-01-20 10:00:15 - INFO - Starting parallel pinning with 50 concurrent operations...
2024-01-20 10:00:35 - INFO - Processing completed in 34.2s:
2024-01-20 10:00:35 - INFO -   User profiles from chain: 150
2024-01-20 10:00:35 - INFO -   Storage requests from chain: 1200
2024-01-20 10:00:35 - INFO -   Total CIDs to pin: 2850
2024-01-20 10:00:35 - INFO -   Successfully pinned: 2847
2024-01-20 10:00:35 - INFO -   Failed to pin: 3
2024-01-20 10:00:35 - INFO -   Average pins per second: 83.2
```

## Monitoring

Monitor the cron job using:

- **Logs**: Check `/var/log/chain-pinner.log` for script output and errors
- **IPFS Status**: Use `ipfs pin ls` to verify pins are being added
- **Substrate Connection**: Script will log connection issues to the log file
- **Cron Status**: Use `crontab -l` to verify the job is scheduled

## Troubleshooting

1. **Substrate Connection Issues**: Check `NODE_URL` and network connectivity
2. **IPFS Connection Issues**: Ensure IPFS daemon is running on the correct port
3. **Permission Issues**: Ensure the user has write access to log files
4. **Memory Issues**: For large datasets, the script processes items incrementally

## Dependencies

- `substrate-interface`: For connecting to Substrate blockchain
- `httpx`: For HTTP requests to IPFS API  
- `base58`: For CID encoding/decoding

## Deployment Notes

This script is designed to be deployed on a separate server from the main validator to:

- Reduce load on the validator server
- Provide redundant IPFS pinning
- Enable independent scaling and monitoring
- Isolate cron job failures from validator operations