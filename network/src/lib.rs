use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{debug, info, warn};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[cfg(test)]
#[path = "tests/network_tests.rs"]
pub mod network_tests;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Network error: {0}")]
    NetworkError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),
}

// Giới hạn kích thước gói tin tối đa, được dùng chung cho cả sender và receiver.
const MAX_FRAME_SIZE: usize = 250 * 1024 * 1024; // 250MB

pub struct NetMessage(pub Bytes, pub Vec<SocketAddr>);

pub struct NetSender {
    transmit: Receiver<NetMessage>,
}

impl NetSender {
    pub fn new(transmit: Receiver<NetMessage>) -> Self {
        Self { transmit }
    }

    pub async fn run(&mut self) {
        let mut senders = HashMap::<_, Sender<_>>::new();
        while let Some(NetMessage(bytes, addresses)) = self.transmit.recv().await {
            for address in addresses {
                let spawn = match senders.get(&address) {
                    Some(tx) => tx.send(bytes.clone()).await.is_err(),
                    None => true,
                };
                if spawn {
                    let tx = Self::spawn_worker(address).await;
                    if let Ok(()) = tx.send(bytes.clone()).await {
                        senders.insert(address, tx);
                    }
                }
            }
        }
    }

    async fn spawn_worker(address: SocketAddr) -> Sender<Bytes> {
        let (tx, mut rx) = channel(10000);
        tokio::spawn(async move {
            let stream = match TcpStream::connect(address).await {
                Ok(stream) => {
                    info!("Outgoing connection established with {}", address);
                    stream
                }
                Err(e) => {
                    warn!("Failed to connect to {}: {}", address, e);
                    return;
                }
            };

            let mut codec = LengthDelimitedCodec::new();
            codec.set_max_frame_length(MAX_FRAME_SIZE);
            let mut transport = Framed::new(stream, codec);

            while let Some(message) = rx.recv().await {
                match transport.send(message).await {
                    Ok(_) => debug!("Successfully sent message to {}", address),
                    Err(e) => {
                        warn!("Failed to send message to {}: {}", address, e);
                        return;
                    }
                }
            }
        });
        tx
    }
}

pub struct NetReceiver<Message> {
    address: SocketAddr,
    deliver: Sender<Message>,
}

impl<Message: 'static + Send + DeserializeOwned + Debug> NetReceiver<Message> {
    pub fn new(address: SocketAddr, deliver: Sender<Message>) -> Self {
        Self { address, deliver }
    }

    pub async fn run(&self) {
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind to TCP port");

        debug!("Listening on {}", self.address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("{}", NetworkError::from(e));
                    continue;
                }
            };
            info!("Incoming connection established with {}", peer);
            Self::spawn_worker(socket, peer, self.deliver.clone()).await;
        }
    }

    async fn spawn_worker(socket: TcpStream, peer: SocketAddr, deliver: Sender<Message>) {
        tokio::spawn(async move {
            let mut codec = LengthDelimitedCodec::new();
            codec.set_max_frame_length(MAX_FRAME_SIZE);
            let mut transport = Framed::new(socket, codec);

            while let Some(frame) = transport.next().await {
                match frame
                    .map_err(NetworkError::from)
                    .and_then(|x| bincode::deserialize(&x).map_err(NetworkError::from))
                {
                    Ok(message) => {
                        debug!("Received {:?}", message);
                        if deliver.send(message).await.is_err() {
                            // Core channel is closed.
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("{}", e);
                        return;
                    }
                }
            }
            warn!("Connection closed by peer {}", peer);
        });
    }
}