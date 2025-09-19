use crate::core::MempoolMessage;
use crate::messages::{Payload, Transaction};
use crypto::{PublicKey, SignatureService};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};
use log::{debug, info};

struct Runner {
    transactions: Vec<Transaction>,
    size: usize,
    max_size: usize,
    min_block_delay: u64,
    name: PublicKey,
    signature_service: SignatureService,
    client_channel: Receiver<Transaction>,
    core_channel: Sender<MempoolMessage>,
    request_channel: Receiver<oneshot::Sender<Payload>>,
}

impl Runner {
    fn new(
        name: PublicKey,
        signature_service: SignatureService,
        max_size: usize,
        min_block_delay: u64,
        client_channel: Receiver<Transaction>,
        core_channel: Sender<MempoolMessage>,
        request_channel: Receiver<oneshot::Sender<Payload>>,
    ) -> Self {
        Self {
            transactions: Vec::with_capacity(max_size),
            size: 0,
            max_size,
            min_block_delay,
            name,
            signature_service,
            client_channel,
            core_channel,
            request_channel,
        }
    }

    async fn add(&mut self, tx: Transaction) -> Option<Payload> {
        let length = tx.len();
        let ret = match self.size + length > self.max_size {
            //nếu vec đầy thì tạo payload
            true => {
                debug!("Buffer full (current size: {}B), creating a new payload.", self.size + length);
                Some(self.make().await)
            },
            false => None,
        };

        self.transactions.push(tx);
        self.size += length;
        ret
    }

    async fn make(&mut self) -> Payload {
        let transactions = self.transactions.drain(..).collect();
        // Cleanup state.
        self.size = 0;
        // Make a payload.
        let payload = Payload::new(transactions, self.name, self.signature_service.clone()).await;
        info!("Created new payload with {} transactions and size {}B.", payload.transactions.len(), payload.size());
        payload
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(transaction) = self.client_channel.recv() => {
                    debug!("Received new transaction from client with size {}B.", transaction.len());
                    if let Some(payload) = self.add(transaction).await {
                        let message = MempoolMessage::OwnPayload(payload);
                        info!("Forwarding new payload to Mempool Core.");
                        if let Err(e) = self.core_channel.send(message).await {
                            panic!("Failed to send payload to the core: {}", e);
                        }

                        // Wait for the minimum block delay.
                        sleep(Duration::from_millis(self.min_block_delay)).await;
                    }
                },
                Some(sender) = self.request_channel.recv() => {
                    let payload = self.make().await;
                    debug!("Consensus requested a payload. Returning payload with {} transactions and size {}B.", payload.transactions.len(), payload.size());
                    let _ = sender.send(payload);
                },
                else => break,
            }
        }
    }
}

pub struct PayloadMaker {
    request_channel: Sender<oneshot::Sender<Payload>>,
}

impl PayloadMaker {
    pub fn new(
        name: PublicKey,
        signature_service: SignatureService,
        max_size: usize,
        min_block_delay: u64,
        client_channel: Receiver<Transaction>,
        core_channel: Sender<MempoolMessage>,
    ) -> Self {
        let (tx_request, rx_request) = channel(10000);
        tokio::spawn(async move {
            Runner::new(
                name,
                signature_service,
                max_size,
                min_block_delay,
                client_channel,
                core_channel,
                rx_request,
            )
            .run()
            .await;
        });
        Self {
            request_channel: tx_request,
        }
    }

    pub async fn make(&mut self) -> Option<Payload> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.request_channel.send(sender).await {
            panic!("Failed to request payload from the inner runner: {}", e);
        }
        let payload = receiver
            .await
            .expect("Failed to receive payload from the inner runner");
        match payload.size() {
            0 => {
                debug!("PayloadMaker returned an empty payload.");
                None
            },
            _ => {
                debug!("PayloadMaker returned a payload with size {}B.", payload.size());
                Some(payload)
            },
        }
    }
}