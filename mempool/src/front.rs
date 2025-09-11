// mempool/src/front.rs

use crate::messages::Transaction;
use futures::stream::StreamExt as _;
use log::{debug, warn, info}; // Thêm info
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct Front {
    address: SocketAddr,
    deliver: Sender<Transaction>,
}

impl Front {
    pub fn new(address: SocketAddr, deliver: Sender<Transaction>) -> Self {
        Self { address, deliver }
    }

    // For each incoming request, we spawn a new worker responsible to receive
    // messages and replay them through the provided deliver channel.
    pub async fn run(&self) {
        //lắng nghe địa chỉ front-end
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind to TCP port");

        debug!("Listening for client transactions on {}", self.address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("Failed to connect with client: {}", e);
                    continue;
                }
            };
            debug!("Connection established with client {}", peer);
            Self::spawn_worker(socket, peer, self.deliver.clone()).await;
        }
    }

    async fn spawn_worker(socket: TcpStream, peer: SocketAddr, deliver: Sender<Transaction>) {
        tokio::spawn(async move {
            let mut transport = Framed::new(socket, LengthDelimitedCodec::new());
            while let Some(frame) = transport.next().await {
                match frame {
                    //nhận tin nhắn được gửi bởi client và lưu vào client_sender
                    Ok(x) => {
                        // --- BƯỚC 1: Giao dịch được nhận ---
                        let tx_size = x.len();
                        if tx_size > 0 {
                            info!(
                                "[BƯỚC 1] Nhận giao dịch từ client {}, kích thước: {} bytes. Chuyển tiếp đến core.",
                                peer, tx_size
                            );
                        } else {
                            warn!("Nhận giao dịch trống từ client {}.", peer);
                        }
                        // --- KẾT THÚC BƯỚC 1 ---

                        deliver.send(x.to_vec()).await.expect("Core channel closed");
                    }
                    Err(e) => {
                        warn!("Failed to receive client transaction: {}", e);
                        return;
                    }
                }
            }
            debug!("Connection closed by client {}", peer);
        });
    }
}