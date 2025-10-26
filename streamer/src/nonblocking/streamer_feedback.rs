use {
    crate::nonblocking::qos::QosControllerWithCensor,
    crossbeam_channel::{bounded, Receiver, Sender, TrySendError},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::HashMap,
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
    tokio_util::{sync::CancellationToken, task::TaskTracker},
};

/// Feedback sent to the QUIC streamer by consumers.
/// This can support different type of receivers.
pub enum StreamerFeedback {
    // Censor the pubkey
    CensorClient(Pubkey),
}

struct ClientCensorInfo {
    censored_time: Instant,
}

pub(crate) struct FeedbackManager<Q>
where
    Q: QosControllerWithCensor,
{
    censored_client: RwLock<HashMap<Pubkey, ClientCensorInfo>>,
    qos: Arc<Q>,
}

impl<Q> FeedbackManager<Q>
where
    Q: QosControllerWithCensor,
{
    pub(crate) fn new(qos: Arc<Q>) -> Self {
        Self {
            censored_client: RwLock::new(HashMap::new()),
            qos,
        }
    }

    pub(crate) fn handle_feedback(&self, feedback: StreamerFeedback) {
        match feedback {
            StreamerFeedback::CensorClient(address) => {
                let mut censored_client: std::sync::RwLockWriteGuard<
                    '_,
                    HashMap<Pubkey, ClientCensorInfo>,
                > = self.censored_client.write().unwrap();
                censored_client.insert(
                    address,
                    ClientCensorInfo {
                        censored_time: Instant::now(),
                    },
                );
                self.qos.censor_client(&address);
            }
        }
    }

    pub(crate) fn uncensor_client(&self, client: &Pubkey) {
        let mut censored_client: std::sync::RwLockWriteGuard<
            '_,
            HashMap<Pubkey, ClientCensorInfo>,
        > = self.censored_client.write().unwrap();
        censored_client.remove(client);
    }

    pub(crate) async fn censor_client(&self, client: &Pubkey) {
        self.qos.censor_client(client).await;
    }
}

pub(crate) fn run_feedback_receiver<Q>(
    feedback_manager: FeedbackManager<Q>,
    feedback_receiver: Receiver<StreamerFeedback>,
    cancel: CancellationToken,
) where
    Q: QosControllerWithCensor,
{
    let feedback_timeout = Duration::from_secs(1);
    info!("Running feedback receiver");
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let feedback = feedback_receiver.recv_timeout(feedback_timeout);
        match feedback {
            Ok(feedback) => {
                feedback_manager.handle_feedback(feedback);
            }
            Err(error) => match error {
                crossbeam_channel::RecvTimeoutError::Timeout => {
                    continue;
                }
                crossbeam_channel::RecvTimeoutError::Disconnected => {
                    break;
                }
            },
        }
    }
}
