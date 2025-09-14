use crate::core::MempoolMessage;
use crate::messages::{Payload, Transaction};
use crypto::{PublicKey, SignatureService};
use log::{info, warn, debug}; // THÊM debug
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
// ---- BẮT ĐẦU THAY ĐỔI ----
use tokio::time::{sleep, Duration, Instant}; // Import thêm Instant
// ---- KẾT THÚC THAY ĐỔI ----

struct Runner {
    transactions: Vec<Transaction>,
    size: usize,
    max_size: usize,
    min_block_delay: Duration, // <-- THAY ĐỔI: Chuyển sang Duration để dễ sử dụng
    min_transactions_in_payload: usize, // THÊM: Ngưỡng giao dịch tối thiểu
    name: PublicKey,
    signature_service: SignatureService,
    client_channel: Receiver<Transaction>,
    core_channel: Sender<MempoolMessage>,
    request_channel: Receiver<oneshot::Sender<Payload>>,
    last_payload_time: Instant, // <-- THÊM VÀO: Biến theo dõi thời gian
}

impl Runner {
    fn new(
        name: PublicKey,
        signature_service: SignatureService,
        max_size: usize,
        min_block_delay_ms: u64, // <-- THAY ĐỔI: Nhận miliseconds
        client_channel: Receiver<Transaction>,
        core_channel: Sender<MempoolMessage>,
        request_channel: Receiver<oneshot::Sender<Payload>>,
    ) -> Self {
        Self {
            transactions: Vec::with_capacity(max_size),
            size: 0,
            max_size,
            min_block_delay: Duration::from_millis(min_block_delay_ms), // <-- THAY ĐỔI
            min_transactions_in_payload: 1000, // ĐẶT MẶC ĐỊNH LÀ 1000 GIAO DỊCH
            name,
            signature_service,
            client_channel,
            core_channel,
            request_channel,
            last_payload_time: Instant::now(), // <-- THÊM VÀO
        }
    }

    // ---- BẮT ĐẦU THAY ĐỔI: Logic tạo payload được tập trung hóa và có delay ----
    async fn make_and_send_payload(&mut self) {
        debug!("[PayloadRunner] Hiện có {} giao dịch ({} B) trước khi kiểm tra delay.", self.transactions.len(), self.size);
        // 1. Kiểm tra xem đã đủ thời gian delay chưa HOẶC đủ số lượng giao dịch chưa
        let elapsed = self.last_payload_time.elapsed();
        let mut num_txs = self.transactions.len(); // Lấy số lượng giao dịch hiện có

        // Chỉ tạo payload nếu đạt min_block_delay HOẶC đủ số lượng giao dịch TỐI THIỂU
        if num_txs < self.min_transactions_in_payload && elapsed < self.min_block_delay {
            let wait_time_delay = self.min_block_delay - elapsed;
            info!("[PayloadRunner] Đợi {} ms để đạt min_block_delay VÀ đủ {} giao dịch tối thiểu.", wait_time_delay.as_millis(), self.min_transactions_in_payload);
            tokio::select! {
                _ = sleep(wait_time_delay) => {},
                Some(transaction) = self.client_channel.recv() => { // Tiếp tục nhận giao dịch trong khi chờ
                    let tx_len = transaction.len();
                    debug!("[PayloadRunner] Trong khi đợi, nhận giao dịch mới ({} B). Tổng số giao dịch hiện tại: {} ({} B).", tx_len, num_txs + 1, self.size + tx_len);
                    self.transactions.push(transaction);
                    self.size += tx_len;
                    // Cập nhật num_txs nếu nhận thêm giao dịch
                    num_txs = self.transactions.len();
                }
            }
        }

        // 2. Tạo payload nếu có đủ giao dịch VÀ đã hết min_block_delay (hoặc do đạt max_size)
        if num_txs >= self.min_transactions_in_payload || (self.size > self.max_size && !self.transactions.is_empty()) || (elapsed >= self.min_block_delay && !self.transactions.is_empty()) {
            info!("[PayloadRunner] Tạo payload: Số lượng giao dịch {} (tối thiểu {}), kích thước {} (tối đa {}).", num_txs, self.min_transactions_in_payload, self.size, self.max_size);
            let transactions = self.transactions.drain(..).collect();
            self.size = 0;
            let payload = Payload::new(transactions, self.name, self.signature_service.clone()).await;
            info!("[PayloadRunner] Payload được tạo với {} txs, kích thước {} B, gửi đến core.", payload.transactions.len(), payload.size());

            let message = MempoolMessage::OwnPayload(payload);
            if let Err(TrySendError::Full(_)) = self.core_channel.try_send(message) {
                 panic!("[PANIC] Kênh từ PayloadRunner đến Core đã đầy!");
            }
        } else {
            debug!("[PayloadRunner] Chưa đủ điều kiện tạo payload. Số lượng giao dịch: {}/{}, Thời gian trôi qua: {:?} / {:?}.", num_txs, self.min_transactions_in_payload, elapsed, self.min_block_delay);
        }
        
        // 3. Cập nhật lại thời gian sau khi đã tạo payload
        self.last_payload_time = Instant::now();
    }
    
    // Hàm này chỉ tạo payload và trả về, dùng cho yêu cầu từ consensus
    async fn make_for_request(&mut self) -> Payload {
        debug!("[PayloadRunner] Nhận yêu cầu từ consensus. Hiện có {} giao dịch ({} B).", self.transactions.len(), self.size);
        let elapsed = self.last_payload_time.elapsed();
        let mut num_txs = self.transactions.len();

        // Nếu chưa đủ giao dịch tối thiểu và chưa đạt min_block_delay, đợi
        if num_txs < self.min_transactions_in_payload && elapsed < self.min_block_delay {
            let wait_time_delay = self.min_block_delay - elapsed;
            info!("[PayloadRunner] Nhận yêu cầu từ consensus, nhưng phải đợi thêm {} ms để đạt min_block_delay VÀ đủ {} giao dịch tối thiểu.", wait_time_delay.as_millis(), self.min_transactions_in_payload);
            tokio::select! {
                _ = sleep(wait_time_delay) => {},
                Some(transaction) = self.client_channel.recv() => {
                    let tx_len = transaction.len();
                    debug!("[PayloadRunner] Trong khi đợi yêu cầu từ consensus, nhận giao dịch mới ({} B). Tổng số giao dịch hiện tại: {} ({} B).", tx_len, num_txs + 1, self.size + tx_len);
                    self.transactions.push(transaction);
                    self.size += tx_len;
                    num_txs = self.transactions.len();
                }
            }
        }

        info!("[PayloadRunner] Tạo payload theo yêu cầu từ consensus. Số lượng giao dịch: {} (tối thiểu {}), kích thước {} (tối đa {}).", num_txs, self.min_transactions_in_payload, self.size, self.max_size);
        let transactions = self.transactions.drain(..).collect();
        self.size = 0;
        let payload = Payload::new(transactions, self.name, self.signature_service.clone()).await;
        info!("[PayloadRunner] Payload được tạo theo yêu cầu với {} txs, kích thước {} B.", payload.transactions.len(), payload.size());
        
        self.last_payload_time = Instant::now();
        payload
    }
// ... existing code ...
    async fn run(&mut self) {
        info!("[PayloadRunner] Vòng lặp cho node {} bắt đầu.", self.name);
        loop {
            tokio::select! {
                Some(transaction) = self.client_channel.recv() => {
                    let tx_len = transaction.len();
                    debug!("[PayloadRunner] Nhận giao dịch mới ({} B). Tổng số giao dịch hiện tại: {} ({} B).", tx_len, self.transactions.len() + 1, self.size + tx_len);
                    // Nếu thêm giao dịch này sẽ làm đầy payload, hãy tạo payload trước.
                    if self.size + tx_len > self.max_size && !self.transactions.is_empty() {
                        info!("[PayloadRunner] Đã đạt max_payload_size ({} B). Tạo payload.", self.max_size);
                        self.make_and_send_payload().await;
                    }
                    self.transactions.push(transaction);
                    self.size += tx_len;
                },
                Some(sender) = self.request_channel.recv() => {
                    info!("[PayloadRunner] Nhận yêu cầu tạo payload từ consensus.");
                    let payload = self.make_for_request().await;
                    if let Err(_) = sender.send(payload) {
                        warn!("[PayloadRunner] Failed to send requested payload back to consensus.");
                    }
                },
                else => {
                    break;
                }
            }
        }
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
        match payload.transactions.len() {
            0 => None,
            _ => Some(payload),
        }
    }
}