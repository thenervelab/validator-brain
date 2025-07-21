# 1. Test API (working) ✅
curl -X POST "https://dht.hippius.com/api/v0/version"

# 2. Test Gateway via HAProxy (should work now) ✅
curl -I "https://dht.hippius.com/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"

# 3. Test findprovs (working) ✅
curl -X POST "https://dht.hippius.com/api/v0/routing/findprovs?arg=bafybeifsu6sz4zstpevzg6so5t2oswrmtnqilhvc2fkd2hkeqsr7pzgezu"

# 4. Test refs (working) ✅
curl -X POST "https://dht.hippius.com/api/v0/refs?arg=QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
