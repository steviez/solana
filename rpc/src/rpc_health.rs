use {
    crate::optimistically_confirmed_bank_tracker::OptimisticallyConfirmedBank,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore::Blockstore,
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    std::{
        collections::HashSet,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
    },
};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum RpcHealthStatus {
    Ok,
    Behind { num_slots: Slot }, // Validator is behind its known validators
    Unknown,
}

pub struct RpcHealth {
    cluster_info: Arc<ClusterInfo>,
    known_validators: Option<HashSet<Pubkey>>,
    optimistically_confirmed_bank: Arc<RwLock<OptimisticallyConfirmedBank>>,
    blockstore: Arc<Blockstore>,
    health_check_slot_distance: u64,
    override_health_check: Arc<AtomicBool>,
    startup_verification_complete: Arc<AtomicBool>,
    #[cfg(test)]
    stub_health_status: std::sync::RwLock<Option<RpcHealthStatus>>,
}

impl RpcHealth {
    pub fn new(
        cluster_info: Arc<ClusterInfo>,
        known_validators: Option<HashSet<Pubkey>>,
        optimistically_confirmed_bank: Arc<RwLock<OptimisticallyConfirmedBank>>,
        blockstore: Arc<Blockstore>,
        health_check_slot_distance: u64,
        override_health_check: Arc<AtomicBool>,
        startup_verification_complete: Arc<AtomicBool>,
    ) -> Self {
        Self {
            cluster_info,
            known_validators,
            optimistically_confirmed_bank,
            blockstore,
            health_check_slot_distance,
            override_health_check,
            startup_verification_complete,
            #[cfg(test)]
            stub_health_status: std::sync::RwLock::new(None),
        }
    }

    pub fn check(&self) -> RpcHealthStatus {
        #[cfg(test)]
        {
            if let Some(stub_health_status) = *self.stub_health_status.read().unwrap() {
                return stub_health_status;
            }
        }

        if !self.startup_verification_complete.load(Ordering::Acquire) {
            return RpcHealthStatus::Unknown;
        }

        let mut old_my_slot = None;
        let mut old_cluster_slot = None;
        let mut old_distance = None;
        let old_result = if self.override_health_check.load(Ordering::Relaxed) {
            RpcHealthStatus::Ok
        } else if let Some(known_validators) = &self.known_validators {
            old_my_slot = self
                .cluster_info
                .get_accounts_hash_for_node(&self.cluster_info.id(), |hashes| {
                    hashes
                        .iter()
                        .max_by(|a, b| a.0.cmp(&b.0))
                        .map(|slot_hash| slot_hash.0)
                })
                .flatten();
            old_cluster_slot = known_validators
                .iter()
                .filter_map(|known_validator| {
                    self.cluster_info
                        .get_accounts_hash_for_node(known_validator, |hashes| {
                            hashes
                                .iter()
                                .max_by(|a, b| a.0.cmp(&b.0))
                                .map(|slot_hash| slot_hash.0)
                        })
                        .flatten()
                })
                .max();

            match (old_my_slot, old_cluster_slot) {
                (
                    Some(latest_account_hash_slot),
                    Some(latest_known_validator_account_hash_slot),
                ) => {
                    old_distance = Some(
                        latest_known_validator_account_hash_slot
                            .saturating_sub(latest_account_hash_slot),
                    );
                    // The validator is considered healthy if its latest account hash slot is within
                    // `health_check_slot_distance` of the latest known validator's account hash slot
                    if latest_account_hash_slot
                        > latest_known_validator_account_hash_slot
                            .saturating_sub(self.health_check_slot_distance)
                    {
                        RpcHealthStatus::Ok
                    } else {
                        let num_slots = latest_known_validator_account_hash_slot
                            .saturating_sub(latest_account_hash_slot);
                        warn!(
                            "health check: behind by {} slots: me={}, latest known_validator={}",
                            num_slots,
                            latest_account_hash_slot,
                            latest_known_validator_account_hash_slot
                        );
                        RpcHealthStatus::Behind { num_slots }
                    }
                }
                (latest_account_hash_slot, latest_known_validator_account_hash_slot) => {
                    if latest_account_hash_slot.is_none() {
                        warn!("health check: latest_account_hash_slot not available");
                    }
                    if latest_known_validator_account_hash_slot.is_none() {
                        warn!(
                            "health check: latest_known_validator_account_hash_slot not available"
                        );
                    }
                    RpcHealthStatus::Unknown
                }
            }
        } else {
            // No known validator point of reference available, so this validator is healthy
            // because it's running
            RpcHealthStatus::Ok
        };

        let my_latest_optimistically_confirmed_slot = self
            .optimistically_confirmed_bank
            .read()
            .unwrap()
            .bank
            .slot();

        let mut optimistic_slot_infos = match self.blockstore.get_latest_optimistic_slots(1) {
            Ok(infos) => infos,
            Err(err) => {
                warn!("health check: blockstore error: {err}");
                return RpcHealthStatus::Unknown;
            }
        };
        let Some((cluster_latest_optimistically_confirmed_slot, _, _)) =
            optimistic_slot_infos.pop()
        else {
            warn!("health check: blockstore does not contain any optimistically confirmed slots");
            return RpcHealthStatus::Unknown;
        };

        let new_distance = cluster_latest_optimistically_confirmed_slot
            .saturating_sub(my_latest_optimistically_confirmed_slot);
        let new_result = if my_latest_optimistically_confirmed_slot
            >= cluster_latest_optimistically_confirmed_slot
                .saturating_sub(self.health_check_slot_distance)
        {
            RpcHealthStatus::Ok
        } else {
            let num_slots = cluster_latest_optimistically_confirmed_slot
                .saturating_sub(my_latest_optimistically_confirmed_slot);
            warn!(
                "health check: behind by {num_slots} \
                slots: me={my_latest_optimistically_confirmed_slot}, \
                latest cluster={cluster_latest_optimistically_confirmed_slot}",
            );
            RpcHealthStatus::Behind { num_slots }
        };

        datapoint_info!(
            "get_health_update_comparison",
            ("old_my_slot", old_my_slot.unwrap_or(u64::MAX), i64),
            ("old_cluster_slot", old_cluster_slot.unwrap_or(u64::MAX), i64),
            ("old_distace", old_distance.unwrap_or(u64::MAX), i64),
            ("old_healthy", (old_result == RpcHealthStatus::Ok) as i64, i64),
            ("new_my_slot", my_latest_optimistically_confirmed_slot, i64),
            (
                "new_cluster_slot",
                cluster_latest_optimistically_confirmed_slot,
                i64
            ),
            ("new_distance", new_distance, i64),
            ("new_healthy", (new_result == RpcHealthStatus::Ok) as i64, i64),
        );

        return new_result;
    }

    #[cfg(test)]
    pub(crate) fn stub() -> Arc<Self> {
        use crate::rpc::tests::new_test_cluster_info;
        Arc::new(Self::new(
            Arc::new(new_test_cluster_info()),
            None,
            42,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(true)),
        ))
    }

    #[cfg(test)]
    pub(crate) fn stub_set_health_status(&self, stub_health_status: Option<RpcHealthStatus>) {
        *self.stub_health_status.write().unwrap() = stub_health_status;
    }
}
