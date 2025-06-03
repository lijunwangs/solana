//! Responsible for monitoring the heartbeat of the Vortexor
//! And take actions if timeout has been dectected.
//! The timeout value is configurable. When timeout, the monitor will
//! fallback to the built-in TPU receiver.
//! The monitor receive hearbeat messages through a crossbeam channel.
//! The heartbeat messages are sent by the Vortexor and forwarded by the
//! Vortexor receiver adapter.

use {
    crate::tpu_switch::TpuSwitch,
    crossbeam_channel::Receiver,
    log::{error, info},
    std::{
        sync::{Arc, RwLock},
        thread::{self, JoinHandle, Result},
        time::{Duration, Instant},
    },
};

/// Heartbeat monitor service.
pub(crate) struct HeartbeatMonitor {
    monitor_thread: JoinHandle<()>,
}

pub type HeartbeatMessage = ();

impl HeartbeatMonitor {
    /// Creates a new HeartbeatMonitor.
    pub fn new(
        timeout: Duration,
        heartbeat_receiver: Receiver<HeartbeatMessage>,
        tpu_switch: Arc<RwLock<TpuSwitch>>,
    ) -> Self {
        info!("Starting heartbeat monitor with timeout: {timeout:?}");
        let monitor_thread = HeartbeatMonitor::start(timeout, tpu_switch, heartbeat_receiver);
        Self { monitor_thread }
    }

    fn switch_to_navite_tpu(tpu_switch: Arc<RwLock<TpuSwitch>>) {
        let mut tpu_switch = tpu_switch.write().unwrap();
        tpu_switch.switch_to_native_tpu();
    }

    fn switch_to_vortexor(tpu_switch: Arc<RwLock<TpuSwitch>>) {
        let mut tpu_switch = tpu_switch.write().unwrap();
        tpu_switch.switch_to_vortexor();
    }

    /// Starts the heartbeat monitor service.
    /// If a timeout is detected, it will trigger a fallback action.
    fn start(
        timeout: Duration,
        tpu_switch: Arc<RwLock<TpuSwitch>>,
        heartbeat_receiver: Receiver<HeartbeatMessage>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            let mut last_heartbeat = Instant::now();

            loop {
                match heartbeat_receiver.recv_timeout(timeout) {
                    Ok(_) => {
                        // Received a heartbeat, update the last heartbeat timestamp.
                        last_heartbeat = Instant::now();
                        info!("Heartbeat received from vortexor. Resetting timeout.",);
                        HeartbeatMonitor::switch_to_vortexor(tpu_switch.clone());
                    }
                    Err(_) => {
                        // Timeout occurred, trigger the fallback action.
                        if last_heartbeat.elapsed() >= timeout {
                            error!("Heartbeat timeout detected. Triggering fallback action.");
                            HeartbeatMonitor::switch_to_navite_tpu(tpu_switch.clone());
                        }
                    }
                }
            }
        })
    }

    pub fn join(self) -> Result<()> {
        self.monitor_thread.join()
    }
}
