use crate::futures::ws::{
    endpoint::MexcFuturesWebsocketEndpoint, message::Message, stream::Stream, subscribe::Subscribe,
    topic::Topic, WebsocketAuth,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct WebsocketEntry {
    pub id: Uuid,
    pub auth: Option<WebsocketAuth>,
    pub listen_key: Option<String>,
    pub topics: Arc<RwLock<Vec<Topic>>>,
    pub message_tx: Arc<RwLock<async_channel::Sender<SendableMessage>>>,
}

#[derive(Debug)]
pub struct Inner {
    pub auth_to_listen_key_map: HashSet<String>,
    pub websockets: Vec<Arc<WebsocketEntry>>,
}

#[derive(Debug, Clone)]
pub struct MexcFuturesWebsocketClient {
    pub inner: Arc<RwLock<Inner>>,
    pub ws_endpoint: Arc<MexcFuturesWebsocketEndpoint>,
    pub broadcast_tx: tokio::sync::broadcast::Sender<Arc<Message>>,
}

#[derive(Debug, serde::Serialize)]
#[serde(untagged)]
pub enum SendableMessage {
    Subscribe(String),
    Unsubscribe(String),
    Ping,
    Login(String),
    Order(String),
}

pub trait MexcFuturesWebsocketClientTrait: Default + Stream + Subscribe + Send + Sync {}
impl MexcFuturesWebsocketClientTrait for MexcFuturesWebsocketClient {}

impl MexcFuturesWebsocketClient {
    pub fn new_with_endpoints(ws_endpoint: MexcFuturesWebsocketEndpoint) -> Self {
        let (broadcast_tx, _broadcast_rx) = tokio::sync::broadcast::channel(1024);

        Self {
            inner: Arc::new(RwLock::new(Inner {
                auth_to_listen_key_map: HashSet::new(),
                websockets: Vec::new(),
            })),
            ws_endpoint: Arc::new(ws_endpoint),
            broadcast_tx,
        }
    }

    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}

impl Default for MexcFuturesWebsocketClient {
    fn default() -> Self {
        Self::new_with_endpoints(MexcFuturesWebsocketEndpoint::Base)
    }
}
