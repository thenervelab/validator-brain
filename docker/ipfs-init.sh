#!/bin/sh
set -e

# Initialize IPFS if not already done
ipfs init --profile server || true

# Add hippius.com as bootstrap peer
ipfs bootstrap add /ip4/172.234.30.30/tcp/4001/p2p/12D3KooWFFhw2tr6A8TvaiJS4QXppg2hQPFh9TaTKGnaq1DifjEr || true

# Start IPFS daemon
exec ipfs daemon --migrate=true --agent-version-suffix=docker