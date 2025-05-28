use crate::futures::error::ApiError;
use crate::futures::ws::acquire_websocket::{
    AcquireWebsocketForTopicsError, AcquireWebsocketsForTopics, AcquireWebsocketsForTopicsParams,
};
use crate::futures::ws::message::RawChannelMessageData;
use crate::futures::ws::public_futures_ws::{MexcFuturesWebsocketClient, SendableMessage};
use crate::futures::ws::topic::{OrderTopic, Topic};
use crate::futures::ws::WebsocketAuth;
use async_channel::SendError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::sync::Arc;
use tracing::trace;

// todo: it is impossible to place order from web socket api
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SendOrderParams {
    pub symbol: String,
    pub price: f64,
    pub vol: u64,
    pub side: Side,
    pub leverage: u64,
    pub open_type: OpenType,
    pub order_type: OrderType,
    //Any uid for identifying your order
    pub external_oid: String,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct RawChannelMsg {
    channel: String,
    pub data: SendOrderParams,
    #[serde(rename = "ts", with = "chrono::serde::ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}

impl RawChannelMsg {
    pub fn new(params: SendOrderParams) -> Self {
        Self {
            channel: "push.personal.order".to_string(),
            data: params,
            timestamp: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DepthTopicParams {
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compress: Option<bool>,
}

#[repr(u8)]
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
#[serde(untagged)]
pub enum Side {
    OpenLong = 1,
    CloseShort = 2,
    OpenShort = 3,
    CloseLong = 4,
}

#[repr(u8)]
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
#[serde(untagged)]
pub enum OrderState {
    Uninformed = 1,
    Uncompleted = 2,
    Completed = 3,
    Cancelled = 4,
    Invalid = 5,
}

#[repr(u8)]
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
#[serde(untagged)]
pub enum OrderType {
    Limit = 1,
    Market = 2,
}

#[repr(u8)]
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug)]
#[serde(untagged)]
pub enum OpenType {
    Isolated = 1,
    Cross = 2,
}

#[derive(Debug, thiserror::Error)]
pub enum SendOrderError {
    /// There is a hard limit of 5 websocket connections per listen key, and a limit of 60 active
    /// listen keys per user id. And each connection can subscribe to up to 30 topics.
    /// Therefore, the maximum number of topics that can be subscribed to per user is 9000.
    ///
    /// It cannot be over 9000!
    #[error("Maximum amount of topics for user will be exceeded")]
    MaximumAmountOfTopicsForUserWillBeExceeded,

    #[error("Requested topics require authentication")]
    RequestedTopicsRequireAuthentication,

    #[error("Tungestenite error: {0}")]
    TungesteniteError(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Could not create datastream (listen key)")]
    CouldNotCreateDataStream(#[from] ApiError),

    #[error("Failed to send message through channel: {0}")]
    SendError(#[from] SendError<SendableMessage>),
}

impl From<AcquireWebsocketForTopicsError> for SendOrderError {
    fn from(value: AcquireWebsocketForTopicsError) -> Self {
        match value {
            AcquireWebsocketForTopicsError::MaximumAmountOfTopicsForUserWillBeExceeded => {
                SendOrderError::MaximumAmountOfTopicsForUserWillBeExceeded
            }
            AcquireWebsocketForTopicsError::RequestedTopicsRequireAuthentication => {
                SendOrderError::RequestedTopicsRequireAuthentication
            }
            AcquireWebsocketForTopicsError::TungesteniteError(err) => {
                SendOrderError::TungesteniteError(err)
            }
            AcquireWebsocketForTopicsError::CouldNotCreateDataStream(err) => {
                SendOrderError::CouldNotCreateDataStream(err)
            }
        }
    }
}

#[async_trait]
pub trait SendOrder {
    async fn send_order(
        self: &Arc<Self>,
        params: SendOrderParams,
        auth: WebsocketAuth,
    ) -> Result<(), SendOrderError>;
}

#[async_trait]
impl SendOrder for MexcFuturesWebsocketClient {
    async fn send_order(
        self: &Arc<Self>,
        params: SendOrderParams,
        auth: WebsocketAuth,
    ) -> Result<(), SendOrderError> {
        let acquire_websocket_params = AcquireWebsocketsForTopicsParams::default()
            .for_topics(vec![Topic::Order(OrderTopic::new())])
            .with_auth(auth);
        let acquire_output = match self
            .clone()
            .acquire_websockets_for_topics(acquire_websocket_params)
            .await
        {
            Ok(x) => x,
            Err(err) => Err(err)?,
        };

        for acquired_ws in acquire_output.websockets.into_iter() {
            let msg = serde_json::to_string(&params).unwrap();

            let sendable_message = SendableMessage::Order(msg);
            trace!("Sending subscription msg: {:?}", sendable_message);
            let tx = acquired_ws.websocket_entry.message_tx.read().await;
            tx.send(sendable_message).await?;

            let mut topics_lock = acquired_ws.websocket_entry.topics.write().await;
            let topics_websocket_entry_does_not_have = acquired_ws
                .for_topics
                .into_iter()
                .filter(|topic| !topics_lock.contains(topic))
                .collect::<Vec<Topic>>();
            topics_lock.extend(topics_websocket_entry_does_not_have);
        }

        Ok(())
    }
}
