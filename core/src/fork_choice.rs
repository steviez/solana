use {
    crate::{
        consensus::{SwitchForkDecision, Tower},
        latest_validator_votes_for_frozen_banks::LatestValidatorVotesForFrozenBanks,
        progress_map::ProgressMap,
        replay_stage::HeaviestForkFailures,
    },
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    std::{
        collections::{HashMap, HashSet},
        sync::{Arc, RwLock},
    },
};

pub struct SelectVoteAndResetForkResult {
    pub vote_bank: Option<(solana_runtime::bank_forks::TrackedArcBank, SwitchForkDecision)>,
    pub reset_bank: Option<solana_runtime::bank_forks::TrackedArcBank>,
    pub heaviest_fork_failures: Vec<HeaviestForkFailures>,
}

pub trait ForkChoice {
    type ForkChoiceKey;
    fn compute_bank_stats(
        &mut self,
        bank: &Bank,
        tower: &Tower,
        latest_validator_votes_for_frozen_banks: &mut LatestValidatorVotesForFrozenBanks,
    );

    // Returns:
    // 1) The heaviest overall bank
    // 2) The heaviest bank on the same fork as the last vote (doesn't require a
    // switching proof to vote for)
    fn select_forks(
        &self,
        frozen_banks: &[solana_runtime::bank_forks::TrackedArcBank],
        tower: &Tower,
        progress: &ProgressMap,
        ancestors: &HashMap<u64, HashSet<u64>>,
        bank_forks: &RwLock<BankForks>,
    ) -> (solana_runtime::bank_forks::TrackedArcBank, Option<solana_runtime::bank_forks::TrackedArcBank>);

    fn mark_fork_invalid_candidate(&mut self, invalid_slot: &Self::ForkChoiceKey);

    /// Returns any newly duplicate confirmed ancestors of `valid_slot` up to and including
    /// `valid_slot` itself
    fn mark_fork_valid_candidate(
        &mut self,
        valid_slot: &Self::ForkChoiceKey,
    ) -> Vec<Self::ForkChoiceKey>;
}
