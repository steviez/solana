#![allow(clippy::arithmetic_side_effects)]

use {
    clap::{crate_description, crate_name, Arg, Command},
    crossbeam_channel::unbounded,
    solana_net_utils::{bind_to_unspecified, SocketConfig},
    solana_streamer::{
        packet::{Packet, PacketBatch, PacketBatchRecycler, PACKET_DATA_SIZE},
        sendmmsg,
        streamer::{receiver, PacketBatchReceiver, StreamerReceiveStats},
    },
    std::{
        cmp::max,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        thread::{sleep, spawn, JoinHandle, Result},
        time::{Duration, Instant},
    },
};

fn producer(addr: &SocketAddr, exit: Arc<AtomicBool>) -> JoinHandle<usize> {
    let send = bind_to_unspecified().unwrap();

    let batch_size = 1024;
    let mut packet_batch = PacketBatch::with_capacity(batch_size);
    packet_batch.resize(batch_size, Packet::default());
    for w in packet_batch.iter_mut() {
        w.meta_mut().size = PACKET_DATA_SIZE;
        w.meta_mut().set_socket_addr(addr);
    }

    spawn(move || {
        let packets_and_addrs: Vec<_> = packet_batch
            .iter()
            .map(|packet| {
                let addr = packet.meta().socket_addr();
                let data = packet.data(..).unwrap();
                (data, addr)
            })
            .collect();

        let mut num_packets_sent = 0;
        while !exit.load(Ordering::Relaxed) {
            sendmmsg::batch_send(&send, &packets_and_addrs).unwrap();
            num_packets_sent += batch_size;
        }
        num_packets_sent
    })
}

fn sink(exit: Arc<AtomicBool>, rvs: Arc<AtomicUsize>, r: PacketBatchReceiver) -> JoinHandle<()> {
    spawn(move || loop {
        if exit.load(Ordering::Relaxed) {
            return;
        }
        let timer = Duration::new(1, 0);
        if let Ok(packet_batch) = r.recv_timeout(timer) {
            rvs.fetch_add(packet_batch.len(), Ordering::Relaxed);
        }
    })
}

fn main() -> Result<()> {
    let mut num_sockets = 1usize;

    let matches = Command::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::new("num-recv-sockets")
                .long("num-recv-sockets")
                .value_name("NUM")
                .takes_value(true)
                .help("Use NUM receive sockets"),
        )
        .arg(
            Arg::new("num-producers")
                .long("num-producers")
                .value_name("NUM")
                .takes_value(true)
                .help("Use this many producer threads."),
        )
        .get_matches();

    if let Some(n) = matches.value_of("num-recv-sockets") {
        num_sockets = max(num_sockets, n.to_string().parse().expect("integer"));
    }

    let num_producers: u64 = matches.value_of_t("num-producers").unwrap_or(4);

    let port = 0;
    let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    let mut addr = SocketAddr::new(ip_addr, 0);

    let exit = Arc::new(AtomicBool::new(false));

    let mut read_channels = Vec::new();
    let mut read_threads = Vec::new();
    let recycler = PacketBatchRecycler::default();
    let (_port, read_sockets) = solana_net_utils::multi_bind_in_range_with_config(
        ip_addr,
        (port, port + num_sockets as u16),
        SocketConfig::default().reuseport(true),
        num_sockets,
    )
    .unwrap();
    let stats = Arc::new(StreamerReceiveStats::new("bench-streamer-test"));
    for read in read_sockets {
        read.set_read_timeout(Some(Duration::new(1, 0))).unwrap();

        addr = read.local_addr().unwrap();
        let (s_reader, r_reader) = unbounded();
        read_channels.push(r_reader);
        read_threads.push(receiver(
            "solRcvrBenStrmr".to_string(),
            Arc::new(read),
            exit.clone(),
            s_reader,
            recycler.clone(),
            stats.clone(),
            Duration::from_millis(1), // coalesce
            true,
            None,
            false,
        ));
    }

    let producer_threads: Vec<_> = (0..num_producers)
        .map(|_| producer(&addr, exit.clone()))
        .collect();

    let rvs = Arc::new(AtomicUsize::new(0));
    let sink_threads: Vec<_> = read_channels
        .into_iter()
        .map(|r_reader| sink(exit.clone(), rvs.clone(), r_reader))
        .collect();

    let start = Instant::now();
    // The threads are already running, so measure now to subtract out later
    let start_val = rvs.load(Ordering::Relaxed);
    sleep(Duration::new(5, 0));
    let elapsed = start.elapsed();
    let end_val = rvs.load(Ordering::Relaxed);

    let packet_per_second = (end_val - start_val) as f64 / elapsed.as_secs_f64();
    println!("packets received per second: {:?}", packet_per_second);

    exit.store(true, Ordering::Relaxed);

    for t_reader in read_threads {
        t_reader.join()?;
    }

    let producer_packet_counts: Vec<_> = producer_threads
        .into_iter()
        .map(|thread_handle| thread_handle.join().unwrap())
        .collect();
    println!(
        "packet production by producer: {:?}",
        producer_packet_counts
    );

    for t_sink in sink_threads {
        t_sink.join()?;
    }
    Ok(())
}
