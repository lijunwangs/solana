//! Module responsible for sending verified transactions out to the registered
//! validators

use {
    agave_banking_stage_ingress_types::{BankingPacketBatch, BankingPacketReceiver},
    crossbeam_channel::RecvTimeoutError,
    log::*,
    solana_streamer::sendmmsg::batch_send,
    std::{
        net::{SocketAddr, UdpSocket},
        sync::{Arc, RwLock},
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
    x509_parser::der_parser::rusticata_macros::debug,
};

pub struct PacketBatchSender {
    thread_hdls: Vec<JoinHandle<()>>,
}

pub const DEFAULT_SENDER_THREADS_COUNT: usize = 8;
pub const DEFAULT_BATCH_SIZE: usize = 128;

pub const DEFAULT_RECV_TIMEOUT: Duration = Duration::from_millis(100);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_PAYLOAD: &[u8] = b"VH";

impl PacketBatchSender {
    pub fn new(
        send_sock: UdpSocket,
        packet_batch_receiver: BankingPacketReceiver,
        num_threads: usize,
        batch_size: usize,
        recv_timeout: Duration,
        destinations: Arc<RwLock<Vec<SocketAddr>>>,
    ) -> Self {
        let thread_hdls = (0..num_threads)
            .map(|thread_id| {
                let packet_batch_receiver = packet_batch_receiver.clone();
                let destinations = destinations.clone();
                let send_sock = send_sock.try_clone().unwrap();
                // let recv_timeout = recv_timeout.clone();
                Builder::new()
                    .name(format!("vtxSdr{thread_id}"))
                    .spawn(move || {
                        Self::recv_send(
                            send_sock,
                            packet_batch_receiver,
                            recv_timeout,
                            batch_size,
                            destinations,
                        );
                    })
                    .unwrap()
            })
            .collect();

        Self { thread_hdls }
    }

    pub fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }

    /// Receive verified packets from the channel `packet_batch_receiver`
    /// and send them to the desintations.
    fn recv_send(
        send_sock: UdpSocket,
        packet_batch_receiver: BankingPacketReceiver,
        recv_timeout: Duration,
        batch_size: usize,
        destinations: Arc<RwLock<Vec<SocketAddr>>>,
    ) {
        let mut last_sent = Instant::now();
        loop {
            let destinations = destinations.read().expect("Expected to get destinations");
            match Self::receive_until(packet_batch_receiver.clone(), recv_timeout, batch_size) {
                Ok((packet_count, packet_batches)) => {
                    if packet_count > 0 {
                        // Collect all packets once for all destinations
                        let mut packets: Vec<&[u8]> = Vec::new();

                        for batch in &packet_batches {
                            for packet_batch in batch.iter() {
                                for packet in packet_batch {
                                    if let Some(data) = packet.data(0..) {
                                        packets.push(data);
                                    }
                                }
                            }
                        }

                        // Send all packets to each destination
                        for destination in destinations.iter() {
                            let packet_refs: Vec<(&[u8], &SocketAddr)> =
                                packets.iter().map(|data| (*data, destination)).collect();
                            let _result = batch_send(&send_sock, packet_refs.into_iter());
                        }
                        last_sent = Instant::now();
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    // If enough time has passed since last sent, send heartbeat
                    if last_sent.elapsed() >= HEARTBEAT_INTERVAL {
                        for destination in destinations.iter() {
                            let result = send_sock.send_to(HEARTBEAT_PAYLOAD, destination);
                            debug!("Sent heartbeat to {destination}: {result:?}",);
                        }
                        debug!("Sent heartbeat to all destinations {:?}", destinations);
                        last_sent = Instant::now();
                    }
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    info!("Exiting the recv_sender as channel is disconnected.");
                    break;
                }
            }
        }
    }

    /// Receives packet batches from sigverify stage with a timeout
    fn receive_until(
        packet_batch_receiver: BankingPacketReceiver,
        recv_timeout: Duration,
        batch_size: usize,
    ) -> Result<(usize, Vec<BankingPacketBatch>), RecvTimeoutError> {
        let start = Instant::now();

        let message = packet_batch_receiver.recv_timeout(recv_timeout)?;
        let packet_batches = &message;
        let num_packets_received = packet_batches
            .iter()
            .map(|batch| batch.len())
            .sum::<usize>();
        let mut messages = vec![message];

        while let Ok(message) = packet_batch_receiver.try_recv() {
            let packet_batches = &message;
            trace!(
                "Got more packet batches in packet receiver: {}",
                packet_batches.len()
            );
            num_packets_received
                .checked_add(
                    packet_batches
                        .iter()
                        .map(|batch| batch.len())
                        .sum::<usize>(),
                )
                .unwrap();
            messages.push(message);

            if start.elapsed() >= recv_timeout || num_packets_received >= batch_size {
                break;
            }
        }

        Ok((num_packets_received, messages))
    }
}
