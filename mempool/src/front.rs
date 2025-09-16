use crate::messages::Transaction;
use futures::stream::StreamExt as _;
use log::{debug, warn};
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
        //监听前端地址
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
            const MAX_FRAME_SIZE: usize = 250 * 1024 * 1024; // Đặt giới hạn là 25MB, lớn hơn 20MB

            let mut codec = LengthDelimitedCodec::new();
            codec.set_max_frame_length(MAX_FRAME_SIZE);
            let mut transport = Framed::new(socket, codec);
            while let Some(frame) = transport.next().await {
                match frame {
                    //接收客户端发送过来的消息 存入client——sender
                    Ok(x) => deliver.send(x.to_vec()).await.expect("Core channel closed"),
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
