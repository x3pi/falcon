use crate::core::MempoolMessage;
use crate::messages::{Payload, Transaction};
use crypto::{PublicKey, SignatureService};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};
use log::info; // Thêm dòng này

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
        let transactions = self.transactions.drain(..).collect();

        // Cleanup state.
        self.size = 0;

        // Make a payload.
        Payload::new(transactions, self.name, self.signature_service.clone()).await
    }

    async fn run(&mut self) {
        info!("[PayloadRunner] Vòng lặp cho node {} bắt đầu.", self.name);
        loop {
            tokio::select! {
                Some(transaction) = self.client_channel.recv() => {
                    info!("[PayloadRunner] Đã nhận giao dịch từ client.");
                    if let Some(payload) = self.add(transaction).await {
                        info!("[PayloadRunner] Payload đã đầy, gửi đến core.");
                        let message = MempoolMessage::OwnPayload(payload);
                        if let Err(e) = self.core_channel.send(message).await {
                            panic!("Không thể gửi payload đến core: {}", e);
                        }

                        // Chờ một khoảng thời gian tối thiểu.
                        sleep(Duration::from_millis(self.min_block_delay)).await;
                    }
                },
                Some(sender) = self.request_channel.recv() => {
                    info!("[PayloadRunner] Nhận yêu cầu tạo payload từ consensus.");
                    let _ = sender.send(self.make().await);
                },
                else => {
                    // Nhánh này được thực thi khi tất cả các kênh đã đóng.
                    break;
                }
            }
        }
        // Dòng này chỉ được thực thi nếu vòng lặp bị phá vỡ.
        panic!("[PayloadRunner] Vòng lặp cho node {} đã dừng đột ngột!", self.name);
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