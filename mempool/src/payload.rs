use crate::core::MempoolMessage;
use crate::messages::{Payload, Transaction};
use crypto::{Hash, PublicKey, SignatureService}; // <-- THÊM 'Hash' VÀO ĐÂY
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};
use log::info;


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
            //如果Vec满了就生成一个payload
            true => Some(self.make().await),
            false => None,
        };

        self.transactions.push(tx);
        self.size += length;
        ret
    }

    async fn make(&mut self) -> Payload {
        // SỬA LỖI: Thêm kiểu dữ liệu tường minh 'Vec<_>' cho 'transactions'
        let transactions:Vec<Vec<u8>>     = self.transactions.drain(..).collect();
        
        let tx_count = transactions.len();

        // Make a payload.
        let payload = Payload::new(transactions, self.name, self.signature_service.clone()).await;

        // Log sau khi tạo payload
        info!(
            "Successfully created payload {} containing {} transactions.",
            payload.digest(),
            tx_count
        );

        payload
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(transaction) = self.client_channel.recv() => {
                    if let Some(payload) = self.add(transaction).await {
                        // --- LOG THÊM VÀO ---
                        info!("Forwarding created payload to mempool core.");
                        // --- KẾT THÚC LOG THÊM VÀO ---

                        let message = MempoolMessage::OwnPayload(payload);
                        if let Err(e) = self.core_channel.send(message).await {
                            panic!("Failed to send payload to the core: {}", e);
                        }

                        // Wait for the minimum block delay.
                        sleep(Duration::from_millis(self.min_block_delay)).await;
                    }
                },
                Some(sender) = self.request_channel.recv() => {
                    // --- LOG THÊM VÀO ---
                    info!("Payload requested by consensus. Triggering payload creation.");
                    // --- KẾT THÚC LOG THÊM VÀO ---
                    let _ = sender.send(self.make().await);
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
            0 => None,
            _ => Some(payload),
        }
    }
}
