use {
    crate::nonblocking::qos::QosControllerWithCensor,
    crossbeam_channel::Receiver,
    solana_pubkey::Pubkey,
    std::{
        collections::HashMap,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::sync::RwLock,
    tokio_util::sync::CancellationToken,
};

/// Feedback sent to the QUIC streamer by consumers.
/// This can support different type of receivers.
pub enum StreamerFeedback {
    // Censor the pubkey
    CensorClient(Pubkey),
    // Uncensor the pubkey
    UncensorClient(Pubkey),
}
#[derive(Debug)]
struct ClientCensorInfo {
    censored_time: Instant,
}

pub(crate) struct FeedbackManager<Q>
where
    Q: QosControllerWithCensor,
{
    censored_client: Arc<RwLock<HashMap<Pubkey, ClientCensorInfo>>>,
    qos: Arc<Q>,
}

impl<Q> FeedbackManager<Q>
where
    Q: QosControllerWithCensor,
{
    pub(crate) fn new(qos: Arc<Q>) -> Self {
        Self {
            censored_client: Arc::new(RwLock::new(HashMap::new())),
            qos,
        }
    }

    pub(crate) async fn handle_feedback(&self, feedback: StreamerFeedback) {
        match feedback {
            StreamerFeedback::CensorClient(address) => {
                self.censor_client(&address).await;
            }
            StreamerFeedback::UncensorClient(address) => {
                self.uncensor_client(&address).await;
            }
        }
    }

    pub(crate) async fn uncensor_client(&self, client: &Pubkey) {
        let mut censored_client = self.censored_client.write().await;
        let removed_client = censored_client.remove(client);
        if let Some(removed_client) = removed_client {
            debug!(
                "Uncensoring with initial censor_time: {:?} pubkey: {client:?}",
                removed_client.censored_time
            );
        }
    }

    pub(crate) async fn censor_client(&self, client: &Pubkey) {
        {
            let mut censored_client = self.censored_client.write().await;
            censored_client.insert(
                client.clone(),
                ClientCensorInfo {
                    censored_time: Instant::now(),
                },
            );
            debug!("Censoring client: {}", client);
            drop(censored_client);
        }
        self.qos.censor_client(client).await;
    }

    pub(crate) async fn is_client_censored(&self, client: &Pubkey) -> bool {
        let censored_client = self.censored_client.read().await;
        censored_client.contains_key(client)
    }
}

pub(crate) async fn run_feedback_receiver<Q>(
    feedback_manager: Arc<FeedbackManager<Q>>,
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
                feedback_manager.handle_feedback(feedback).await;
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
