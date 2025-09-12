// mempool/src/front.rs

use crate::messages::Transaction;
use futures::stream::StreamExt as _;
use log::{debug, warn}; // Thêm info
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio::sync::mpsc::error::TrySendError;

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
                    Ok(x) => {
                        // THAY ĐỔI
                        if let Err(e) = deliver.try_send(x.to_vec()) {
                            match e {
                                TrySendError::Full(_) => {
                                    warn!("[BACK-PRESSURE] Kênh deliver từ Front đến PayloadRunner đã đầy. Từ chối giao dịch từ {}.", peer);
                                    // Đóng kết nối để báo cho client biết rằng node đang bị quá tải.
                                    return;
                                },
                                TrySendError::Closed(_) => {
                                    warn!("[WARN] Kênh deliver từ Front đến PayloadRunner đã bị đóng.");
                                    return;
                                }
                            }
                        }
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