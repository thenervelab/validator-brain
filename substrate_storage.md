substrate node is local or can be remote

get all miner profile: ipfsPallet.minerProfile: Bytes > example miner_profile_list.json

get all miners state: ipfsPallet.minerStates: IpfsPalletMinerState > miner_state_list.json

get ipfsPallet.minerTotalFilesPinned: Option<u32> > miner_total_files_pinned.json

get ipfsPallet.minerTotalFilesSize: Option<u128> > miner_total_files_size.json

get ipfsPallet.rebalanceRequest: Vec<IpfsPalletRebalanceRequestItem> > rebalance_request_list.json

get ipfsPallet.pinningRequest: Vec<IpfsPalletPinningRequestItem> > pinning_request_list.json

get ipfsPallet.unpinRequests: Vec<Bytes>
 > unpinning_request_list.json

get ipfsPallet.userProfile: Bytes > user_profile_list.json

user profile example > user_profile_example.json

single miner profile > single_miner_profile.json