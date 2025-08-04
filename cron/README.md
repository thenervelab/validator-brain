# Chain Data Pinner Cron Script

This standalone cron script fetches user profiles and storage requests from the Substrate blockchain
and pins them to a local IPFS node.

## Overview

The script runs independently of the main validator orchestrator and performs the following tasks:

1. **Fetches User Profiles**: Queries `IpfsPallet::UserProfile` storage to get all user profile CIDs
2. **Processes Profile Contents**: Downloads each user profile JSON and extracts individual file
   CIDs from the `file_hash` fields
3. **Fetches Storage Requests**: Queries `IpfsPallet::UserStorageRequests` storage to get all file
   CIDs from storage requests
4. **Pins to IPFS**: Pins all discovered CIDs (profile CIDs + individual file CIDs + storage request
   CIDs) to the local IPFS node
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

- `NODE_URL`: Substrate RPC endpoint
- `IPFS_API_URL`: Local IPFS API endpoint (default: `http://127.0.0.1:5001`)
- `IPFS_GATEWAY_URL`: IPFS gateway for fetching profile contents (default: `http://127.0.0.1:5001`)

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