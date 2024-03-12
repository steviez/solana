//! this service asynchronously reports CostTracker stats

use {
    crossbeam_channel::Receiver,
    solana_ledger::blockstore::Blockstore,
    solana_runtime::bank::Bank,
    std::{
        sync::Arc,
        thread::{self, Builder, JoinHandle},
    },
};
pub enum CostUpdate {
    FrozenBank { bank: Arc<Bank> },
}

pub type CostUpdateReceiver = Receiver<CostUpdate>;

pub struct CostUpdateService {
    thread_hdl: JoinHandle<()>,
}

impl CostUpdateService {
    pub fn new(blockstore: Arc<Blockstore>, cost_update_receiver: CostUpdateReceiver) -> Self {
        let thread_hdl = Builder::new()
            .name("solCostUpdtSvc".to_string())
            .spawn(move || {
                Self::service_loop(blockstore, cost_update_receiver);
            })
            .unwrap();

        Self { thread_hdl }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }

    fn service_loop(blockstore: Arc<Blockstore>, cost_update_receiver: CostUpdateReceiver) {
        let sleep = std::time::Duration::from_millis(100);

        use std::collections::HashMap;
        let mut bank_cache: HashMap<u64, Arc<Bank>> = HashMap::default();
        loop {
            std::thread::sleep(sleep);
            match cost_update_receiver.try_recv() {
                Err(crossbeam_channel::TryRecvError::Disconnected) => return,
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    continue;
                }
                Ok(CostUpdate::FrozenBank { bank }) => {
                    bank_cache.insert(bank.slot(), bank);
                }
            };

            let max_root = blockstore.max_root();
            bank_cache.retain(|&slot, bank| {
                // Keep anything newer than the max root
                let retain = slot > max_root;
                // If bank is <= max_root, we can know if it was rooted or not
                if !retain {
                    let is_root = blockstore.is_root(slot);
                    bank.read_cost_tracker()
                        .unwrap()
                        .report_stats(bank.slot(), is_root);
                }
                retain
            })
        }
    }
}
