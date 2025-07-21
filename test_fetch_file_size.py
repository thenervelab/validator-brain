import asyncio

from rabbitmq.pinning_request_consumer import fetch_ipfs_file_size

cid = "bafkreiacxv3dy4oc7uf6v7tk7dtr5zei4u72wlg2l5bpla5yaczcgbhoma"

if __name__ == "__main__":
    asyncio.run(fetch_ipfs_file_size(cid))
