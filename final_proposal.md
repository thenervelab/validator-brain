
```markdown
# Hippius Validator Off-Chain Consensus and Pinning Orchestration

**Version:** 1.0
**Date:** May 10, 2025

## 1. Overview

This document outlines the off-chain process used by Hippius validators to reach consensus on assigning file replicas to storage miners. The system prioritizes decentralization among validators for proposing and agreeing on a pinning plan, utilizing Automerge CRDTs for state aggregation and IPFS for storing agreed-upon state. The final consensus on the validity of a plan is achieved on the Substrate chain by comparing hashes of the proposed plan.

**Goals:**

*   Decentralize the decision-making for file replica assignments among validators.
*   Ensure all validators operate on a consistent view of the network state and proposed actions.
*   Leverage the Substrate chain for final consensus on the pinning plan and for critical operations like miner capacity locking.
*   Prioritize assigning replicas to underutilized or new miners.

**Core Components:**

*   **Hippius Validators (5):** Nodes responsible for executing this off-chain logic.
*   **Substrate Chain:** Source of truth for pinning requests, miner registration, on-chain locks, and final plan consensus.
*   **IPFS Network:** Used to store miner profiles, user profiles, file data, and snapshots of agreed-upon CRDT states.
*   **Automerge CRDTs:** Used by validators to build and merge proposals, and to disseminate the final agreed-upon pinning plan.
*   **Private P2P Network (libp2p):** For communication between the 5 validators to exchange Automerge changesets.
*   **Local Validator Cache/Index (Optional, e.g., local PostgreSQL/SQLite):** Each validator can maintain a local cache of chain/IPFS data for efficient querying and proposal generation.

## 2. Key Data Structures (Managed with Automerge by Validators)

### 2.1. `current_miner_workload_and_status` (Automerge Document)

*   **Purpose:** Provides a shared, eventually consistent view of each miner's status and workload as observed/updated by validators. This serves as input for proposal generation.
*   **Update Frequency:** Continuously updated by validators based on their health checks and results of previous pinning rounds.
*   **Structure (Map: `miner_id` -> `miner_data`):**
    ```json
    {
      "miner_X_id": {
        "profile_cid": "Qm...", // CID of their self-declared profile on IPFS
        "observed_status": "online" | "offline" | "unknown",
        "last_status_update_by": "validator_A_id",
        "last_status_timestamp": "...",
        "hippius_pinned_cids_list_cid": "Qm...", // Optional: CID of an Automerge doc listing CIDs this miner is supposed to pin FOR HIPPIUS
        "hippius_pinned_count": 12, // Derived from the list above or direct count
        "estimated_hippius_used_gb": 250,
        "calculated_free_space_gb": 750 // Based on profile_cid data minus hippius_used_gb
      },
      // ... for all ~600 miners
    }
    ```
*   **Conflict Resolution:** Automerge handles concurrent updates (e.g., last writer wins for fields like `observed_status` or use a dedicated CRDT list for status history).

### 2.2. `epoch_X_pinning_proposals` (Automerge Document - Per Epoch)

*   **Purpose:** Aggregates proposals from all validators for a specific pinning/rebalancing epoch (X). Reset or archived after each epoch.
*   **Structure (Map: `validator_id` -> `validator_proposal`):**
    ```json
    {
      "validator_A_id": {
        "new_assignments_proposed": [
          { "cid_to_pin": "QmNewFile1", "target_miners_preferred": ["miner_X_id", "miner_Y_id", "miner_Z_id", "miner_P_id", "miner_Q_id"], "justification_score": 0.95 },
          // ... other new files
        ],
        "rebalance_assignments_proposed": [
          { "cid_to_rebalance": "QmOldFile1", "offline_miner": "miner_Offline_id", "new_target_miners_preferred": ["miner_R_id", ...], "justification_score": 0.98 }
        ],
        "miner_status_observations": { // Validator A's specific observations for this epoch
            "miner_S_id": { "observed_status": "online", "checked_at": "..." }
        }
      },
      // ... proposals from validator_B_id, etc.
    }
    ```

### 2.3. `finalized_epoch_X_pinning_plan` (Automerge Document - Per Epoch)

*   **Purpose:** Contains the single, deterministically generated pinning plan for epoch X, created by the epoch leader. This is the document whose hash is submitted to the chain.
*   **Structure:**
    ```json
    {
      "epoch_id": "X",
      "leader_validator_id": "validator_L_id",
      "based_on_proposals_cid": "Qm...", // CID of the IPFS-stored `epoch_X_pinning_proposals` document
      "plan_generation_timestamp": "...",
      "pinning_plan": { // The actual assignments
        "QmNewFile1": ["miner_selected_A", "miner_selected_B", "miner_selected_C", "miner_selected_D", "miner_selected_E"],
        "QmOldFile2_rebalanced": ["miner_selected_F", ...],
        // ... for all CIDs processed in this epoch
      },
      "miner_final_status_for_epoch": { // Optional: Consensus view on miner status used for this plan
          "miner_S_id": "online",
          "miner_T_id": "offline"
      }
    }
    ```

## 3. Validator P2P Communication

*   Validators (5) will form a **private `libp2p` swarm**.
*   A **swarm key** will be used to ensure privacy.
*   At least one, preferably two, validators (or dedicated lightweight nodes) will act as **bootstrap nodes** for this private swarm.
*   Automerge changesets for the documents listed above (especially `current_miner_workload_and_status` and `epoch_X_pinning_proposals`, and then the `finalized_epoch_X_pinning_plan`) will be exchanged using `libp2p`'s pub/sub mechanism, scoped to a private topic within this swarm.
*   All exchanged Automerge changesets should be wrapped in a message signed by the sending validator's key for an additional layer of sender verification if desired, though libp2p secure channels also provide peer authentication.

## 4. Epoch-Based Consensus Workflow

**Phase 0: Ongoing - Maintain `current_miner_workload_and_status`**

1.  **Health Checks:** Each validator is assigned a subset of the ~600 miners to monitor (e.g., 120 miners each). Assignment logic is deterministic (e.g., based on sorted miner IDs and validator index).
2.  **Status Updates:** Validators perform health checks (ping, IPFS `dag stat` or random block `get` on a known pinned file) on their assigned miners.
3.  **CRDT Update:** Validators update the relevant miner entries in the `current_miner_workload_and_status` Automerge document with their observations. Changesets are broadcast to other validators.
4.  **Profile Fetching:** Validators periodically fetch miner profile CIDs (from chain or the `current_miner_workload_and_status` doc) and retrieve/cache profile data from IPFS to understand capacity.

**Phase 1: Epoch Initialization & Proposal Generation**

1.  **Epoch Trigger:** An epoch begins (e.g., fixed time interval, new block height range).
2.  **Leader Election:** A leader validator for the current epoch (epoch X) is deterministically chosen (e.g., `leader_id = sorted_validator_ids[X % num_validators]`). All validators know who the leader is.
3.  **Identify Work:** Validators query the Substrate chain for new pinning requests and identify CIDs needing rebalancing (based on their view of `current_miner_workload_and_status` showing offline miners with assigned CIDs).
4.  **Local Proposal Calculation:** Each validator:
    *   Uses its local cache (PostgreSQL/SQLite) and its up-to-date view of `current_miner_workload_and_status`.
    *   For each CID needing pinning/rebalancing, it selects its preferred 5 target miners, prioritizing underutilized and online miners.
    *   Constructs its proposal object for the `epoch_X_pinning_proposals` document.
5.  **Publish Proposal:** Each validator adds/updates its entry in the `epoch_X_pinning_proposals` Automerge document and broadcasts the changesets.

**Phase 2: Proposal Aggregation**

1.  **Merge Proposals:** All validators apply incoming changesets for `epoch_X_pinning_proposals`. They all converge to an identical Automerge document containing all 5 validator proposals and their miner status observations for this epoch.
2.  **(Optional Snapshot):** The epoch leader (or all validators) can choose to pin the fully merged `epoch_X_pinning_proposals` document to IPFS for auditability. The resulting CID can be stored.

**Phase 3: Leader Generates Final Pinning Plan**

1.  **Leader Action:** The elected epoch leader takes the converged `epoch_X_pinning_proposals` document as input.
2.  **Deterministic Algorithm:** The leader runs a pre-defined, deterministic algorithm to generate the `finalized_epoch_X_pinning_plan.pinning_plan`. This algorithm:
    *   Iterates through all CIDs needing placement.
    *   For each CID, it considers all proposed target miners from all validators.
    *   It filters candidates based on consolidated miner status (derived from all proposals, e.g., majority vote on online/offline, average workload).
    *   It scores remaining candidates, heavily prioritizing available capacity and low current Hippius workload.
    *   It selects exactly 5 miners per CID.
    *   It employs deterministic tie-breaking (e.g., sorting by miner ID).
    *   It ensures a miner is not over-allocated *within this current epoch's plan* beyond a certain threshold of new assignments.
3.  **Create `finalized_epoch_X_pinning_plan`:** The leader populates the `finalized_epoch_X_pinning_plan` Automerge document with the generated plan, its ID, epoch ID, and the (optional) IPFS CID of the proposals document it was based on.
4.  **Broadcast Final Plan:** The leader broadcasts the Automerge changesets for `finalized_epoch_X_pinning_plan` to all other validators.

**Phase 4: Verification & Submission to Chain**

1.  **Receive Final Plan:** All validators apply the changesets and now possess the identical `finalized_epoch_X_pinning_plan` document.
2.  **Store Final Plan on IPFS:** Each validator (or just the leader) pins the `finalized_epoch_X_pinning_plan` document (the full Automerge document content) to IPFS. They will all arrive at the **same IPFS CID** for this document because its content is identical.
3.  **Submit Hash to Chain:** Each of the 5 validators submits the **IPFS CID** of the `finalized_epoch_X_pinning_plan` to a specific function in a Substrate smart contract.
4.  **On-Chain Consensus:**
    *   The smart contract waits to receive submissions.
    *   Once it receives at least a threshold (e.g., 3 out of 5, or >2/3) of **identical IPFS CIDs** for the epoch's plan, it considers the plan "approved" by validator consensus.
    *   The smart contract can then emit an event signifying the approved plan's IPFS CID.
5.  **Chain Actions (Triggered by Approved Plan):**
    *   One or more validators (perhaps the leader, or any validator seeing the "approved" event) are now authorized to execute the on-chain actions based on the plan referenced by the approved IPFS CID.
    *   This involves:
        *   Fetching the `finalized_epoch_X_pinning_plan` from IPFS using the approved CID.
        *   Interacting with the Substrate chain to lock capacity on the designated miners for the specified CIDs.
        *   Updating the status of pinning requests on the chain to "resolved" or "in_progress_with_miners_X_Y_Z".

**Phase 5: Post-Epoch State Updates**

1.  **Update `current_miner_workload_and_status`:** Based on the successful on-chain locks and assignments from the approved plan, validators update the workload information in this Automerge document.
2.  **Cleanup:** Archive or reset `epoch_X_pinning_proposals`.

## 5. Unpin and Rebalance Requests

*   **Unpin Requests:** Originate from the chain. A leader (or assigned validator) generates a plan (which CIDs to remove from which miners), proposes it in an Automerge doc (similar to `finalized_epoch_X_pinning_plan` but for unpins), gets consensus via hash submission to chain, then executes removal (which might mean updating chain state about locks and miner profiles).
*   **Rebalance Requests:** Triggered by validators detecting offline miners (Phase 0). The process follows Phases 1-5, where "new assignments" are for the CIDs that need re-replication.

## 6. Security Considerations

*   **Private Swarm Key:** Must be kept secure. Compromise means an attacker can join the validator P2P network.
*   **Validator Keys:** Standard security practices for validator node keys.
*   **Deterministic Algorithm:** The code for the leader's plan generation algorithm must be identical across validators (if they are to verify it) and auditable.
*   **Chain Interaction:** All Substrate transactions must be signed by authorized validator keys.
*   **IPFS Data Integrity:** While CIDs ensure data integrity, ensure validators fetch the *correct* IPFS CID for profiles and plans (e.g., from a trusted chain event or a signed message).

## 7. Future Considerations

*   Slashing conditions for validators failing to participate or submitting malicious/incorrect plan hashes.
*   More sophisticated scoring and weighting in the leader's deterministic algorithm.
*   Optimizing the size and frequency of Automerge changeset exchanges.

This specification outlines a robust, decentralized method for your 5 validators to collaboratively build and agree upon a complex operational plan, leveraging CRDTs for off-chain state merging and the Substrate chain for the final layer of trust and consensus.
```