//! Administrative functions to manipulate the Blockstore that are not necessary
//! for normal operation
use {
    crate::blockstore::{Blockstore, Result},
    solana_clock::Slot,
    std::borrow::Cow,
};

impl Blockstore {
    /// Copy a slot from this Blockstore to `target_blockstore`
    pub fn copy_slot(&self, target_blockstore: &Blockstore, slot: Slot) -> Result<()> {
        let shreds = self.get_data_shreds_for_slot(slot, 0)?;
        let shreds = shreds.into_iter().map(Cow::Owned);

        target_blockstore.insert_cow_shreds(shreds, None, true)?;

        Ok(())
    }
}
