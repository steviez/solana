//! Threadpool
use {
    crate::{packet::PacketBatch, sigverify::verify_packet},
    crossbeam_channel::{unbounded, Receiver, Sender},
    std::{
        cmp::min,
        num::NonZeroUsize,
        thread::{Builder, JoinHandle},
    },
};

// Thread names can be a maximum of 16 characters on Linux
// One of those characters is the null terminator, reserve two more characters for an index
const MAX_THREAD_NAME_LEN: usize = 13;

pub struct ThreadPool {
    dispatch_sender: Sender<PacketBatch>,
    dispatch_receiver: Receiver<PacketBatch>,
    workers: Vec<JoinHandle<()>>,
}

impl ThreadPool {
    fn worker(receiver: Receiver<PacketBatch>, sender: Sender<PacketBatch>, reject_non_vote: bool) {
        loop {
            let Ok(mut batch) = receiver.recv() else {
                return;
            };

            for packet in batch.iter_mut() {
                if !packet.meta().discard() && !verify_packet(packet, reject_non_vote) {
                    packet.meta_mut().set_discard(true);
                }
            }

            let Ok(_) = sender.send(batch) else {
                return;
            };
        }
    }

    pub fn new(name: &str, size: NonZeroUsize, reject_non_vote: bool) -> Self {
        let name = &name[0..min(name.len(), MAX_THREAD_NAME_LEN)];
        let (dispatch_sender, worker_receiver) = unbounded();
        let (worker_sender, dispatch_receiver) = unbounded();

        let workers = (0..size.get())
            .map(|index| {
                let name = format!("{name}{index:02}");
                let receiver = worker_receiver.clone();
                let sender = worker_sender.clone();
                Builder::new()
                    .name(name.clone())
                    .spawn(move || {
                        info!("thread {name} starting");
                        Self::worker(receiver, sender, reject_non_vote);
                        info!("thread {name} stopping");
                    })
                    .expect("spawn thread")
            })
            .collect();

        Self {
            dispatch_sender,
            dispatch_receiver,
            workers,
        }
    }

    pub fn do_work(&self, batches: &mut Vec<PacketBatch>) {
        let num_batches = batches.len();

        let batches_in = std::mem::take(batches);
        for batch in batches_in.into_iter() {
            self.dispatch_sender.send(batch).unwrap();
        }

        let mut batches_out = self.dispatch_receiver.iter().take(num_batches).collect();
        std::mem::swap(batches, &mut batches_out);
    }

    pub fn join(self) {
        drop(self.dispatch_sender);
        drop(self.dispatch_receiver);
        for worker in self.workers.into_iter() {
            worker.join().expect("join thread");
        }
    }
}
