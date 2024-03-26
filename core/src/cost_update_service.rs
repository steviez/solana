//! this service asynchronously reports CostTracker stats

use {
    crossbeam_channel::Receiver,
    solana_ledger::blockstore::Blockstore,
    solana_runtime::bank::Bank,
    std::{
        sync::Arc,
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};
pub enum CostUpdate {
    FrozenBank { bank: Arc<Bank> },
}

pub type CostUpdateReceiver = Receiver<CostUpdate>;

pub struct CostUpdateService {
    thread_hdl: JoinHandle<()>,
}

// The maximum number of retries to check if CostTracker::in_flight_transaction_count() has settled
// to zero. Bail out after this many retries; the in-flight count is reported so this is ok
const MAX_LOOP_COUNT: usize = 25;
// Throttle checking the count
const LOOP_LIMITER: Duration = Duration::from_millis(10);

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

    fn service_loop(_blockstore: Arc<Blockstore>, cost_update_receiver: CostUpdateReceiver) {
        for cost_update in cost_update_receiver.iter() {
            match cost_update {
                CostUpdate::FrozenBank { bank } => {
                    for loop_count in 0..MAX_LOOP_COUNT {
                        // Release the lock so that the thread that will update
                        // the count is able to obtain a write lock
                        let in_flight_transaction_count = bank
                            .read_cost_tracker()
                            .unwrap()
                            .in_flight_transaction_count();
                        if in_flight_transaction_count == 0 {
                            let slot = bank.slot();
                            trace!("inflight transaction count for slot {slot} settled to zero \
                                after {loop_count} iterations");
                            break;
                        }

                        std::thread::sleep(LOOP_LIMITER);
                    }

                    let cost_tracker = bank.read_cost_tracker().unwrap();
                    cost_tracker.report_stats(bank.slot());
                }
            }
        }
    }
}
