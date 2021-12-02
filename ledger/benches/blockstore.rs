#![allow(clippy::integer_arithmetic)]
#![feature(test)]
extern crate solana_ledger;
extern crate test;

use rand::Rng;
use solana_entry::entry::{create_ticks, Entry};
use solana_ledger::{
    blockstore::{entries_to_test_shreds, Blockstore},
    get_tmp_ledger_path_auto_delete,
    shred::max_ticks_per_n_shreds,
};
use solana_sdk::{clock::Slot, hash::Hash};
use test::Bencher;

// Create the entries necessary to occupy `num_shreds` shreds
fn create_entries_for_n_shreds(num_shreds: u64) -> Vec<Entry> {
    // Use `None` as shred_size so the default (full) value is used
    let num_ticks = max_ticks_per_n_shreds(num_shreds, None);
    create_ticks(num_ticks, 0, Hash::default())
}

// Given some shreds and a ledger at ledger_path, benchmark writing the shreds to the ledger
fn do_bench_write_shreds(bencher: &mut Bencher, num_shreds: u64) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore = Blockstore::open(ledger_path.path()).unwrap();
    let entries = create_entries_for_n_shreds(num_shreds);

    let mut slot = 0;
    //let mut metrics = BlockstoreInsertionMetrics::default();
    bencher.iter(move || {
        let shreds = entries_to_test_shreds(entries.clone(), slot, slot.saturating_sub(1), true, 0);
        let shreds_len = shreds.len();
        blockstore.insert_shreds(shreds, None, false).unwrap();
        // Increment slot as we go since duplicates get thrown out
        slot += 1;
    });
}

// Insert some shreds into the ledger in preparation for read benchmarks
fn setup_read_bench(
    blockstore: &Blockstore,
    num_small_shreds: u64,
    num_large_shreds: u64,
    slot: Slot,
) {
    // Make some big and small entries
    let entries = create_ticks(
        num_large_shreds * 4 + num_small_shreds * 2,
        0,
        Hash::default(),
    );

    // Convert the entries to shreds, write the shreds to the ledger
    let shreds = entries_to_test_shreds(entries, slot, slot.saturating_sub(1), true, 0);
    blockstore
        .insert_shreds(shreds, None, false)
        .expect("Expectd successful insertion of shreds into ledger");
}

#[bench]
#[ignore]
fn bench_write_shreds_1000_per_slot(bencher: &mut Bencher) {
    solana_logger::setup();
    do_bench_write_shreds(bencher, 1000);
}

#[bench]
#[ignore]
fn bench_read_sequential(bencher: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore = Blockstore::open(ledger_path.path()).unwrap();

    // Insert some big and small shreds into the ledger
    let num_small_shreds = 32 * 1024;
    let num_large_shreds = 32 * 1024;
    let total_shreds = num_small_shreds + num_large_shreds;
    let slot = 0;
    setup_read_bench(&blockstore, num_small_shreds, num_large_shreds, slot);

    let num_reads = total_shreds / 15;
    let mut rng = rand::thread_rng();
    bencher.iter(move || {
        // Generate random starting point in the range [0, total_shreds - 1], read num_reads shreds sequentially
        let start_index = rng.gen_range(0, num_small_shreds + num_large_shreds);
        for i in start_index..start_index + num_reads {
            let _ = blockstore.get_data_shred(slot, i as u64 % total_shreds);
        }
    });
}

#[bench]
#[ignore]
fn bench_read_random(bencher: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore = Blockstore::open(ledger_path.path()).unwrap();

    // Insert some big and small shreds into the ledger
    let num_small_shreds = 32 * 1024;
    let num_large_shreds = 32 * 1024;
    let total_shreds = num_small_shreds + num_large_shreds;
    let slot = 0;
    setup_read_bench(&blockstore, num_small_shreds, num_large_shreds, slot);

    let num_reads = total_shreds / 15;

    // Generate a num_reads sized random sample of indexes in range [0, total_shreds - 1],
    // simulating random reads
    let mut rng = rand::thread_rng();
    let indexes: Vec<usize> = (0..num_reads)
        .map(|_| rng.gen_range(0, total_shreds) as usize)
        .collect();
    bencher.iter(move || {
        for i in indexes.iter() {
            let _ = blockstore.get_data_shred(slot, *i as u64);
        }
    });
}
