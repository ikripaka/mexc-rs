use crate::futures::error::ApiError;
use crate::futures::ws::acquire_websocket::{
    AcquireWebsocketForTopicsError, AcquireWebsocketsForTopics, AcquireWebsocketsForTopicsParams,
};
use crate::futures::ws::public_futures_ws::{MexcFuturesWebsocketClient, SendableMessage};
use crate::futures::ws::topic::Topic;
use crate::futures::ws::WebsocketAuth;
use async_channel::SendError;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::trace;

#[derive(Debug)]
pub struct SubscribeParams {
    pub topics: Vec<Topic>,
    pub auth: Option<WebsocketAuth>,
}

impl Default for SubscribeParams {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl SubscribeParams {
    pub fn new(topics: Vec<Topic>) -> Self {
        Self { topics, auth: None }
    }

    pub fn with_auth(mut self, auth: WebsocketAuth) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn with_topic(mut self, topic: Topic) -> Self {
        self.topics.push(topic);
        self
    }

    pub fn with_topics(mut self, topics: Vec<Topic>) -> Self {
        self.topics.extend(topics);
        self
    }
}

#[derive(Debug)]
pub struct SubscribeOutput {}

#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
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

impl From<AcquireWebsocketForTopicsError> for SubscribeError {
    fn from(value: AcquireWebsocketForTopicsError) -> Self {
        match value {
            AcquireWebsocketForTopicsError::MaximumAmountOfTopicsForUserWillBeExceeded => {
                SubscribeError::MaximumAmountOfTopicsForUserWillBeExceeded
            }
            AcquireWebsocketForTopicsError::RequestedTopicsRequireAuthentication => {
                SubscribeError::RequestedTopicsRequireAuthentication
            }
            AcquireWebsocketForTopicsError::TungesteniteError(err) => {
                SubscribeError::TungesteniteError(err)
            }
            AcquireWebsocketForTopicsError::CouldNotCreateDataStream(err) => {
                SubscribeError::CouldNotCreateDataStream(err)
            }
        }
    }
}

#[async_trait]
pub trait Subscribe {
    async fn subscribe(
        self: Arc<Self>,
        params: SubscribeParams,
    ) -> Result<SubscribeOutput, SubscribeError>;
}

#[async_trait]
impl Subscribe for MexcFuturesWebsocketClient {
    async fn subscribe(
        self: Arc<Self>,
        params: SubscribeParams,
    ) -> Result<SubscribeOutput, SubscribeError> {
        let mut acquire_websocket_params =
            AcquireWebsocketsForTopicsParams::default().for_topics(params.topics);
        if let Some(auth) = params.auth.as_ref() {
            acquire_websocket_params = acquire_websocket_params.with_auth(auth.clone());
        }
        let acquire_output = match self
            .clone()
            .acquire_websockets_for_topics(acquire_websocket_params)
            .await
        {
            Ok(x) => x,
            Err(err) => Err(err)?,
        };

        for acquired_ws in acquire_output.websockets.into_iter() {
            let local_params = acquired_ws
                .for_topics
                .iter()
                .map(|topic| (topic.clone(), topic.to_subscription_msg()))
                .collect::<Vec<(Topic, String)>>();

            for (t, x) in local_params {
                let tx = acquired_ws.websocket_entry.message_tx.read().await;
                if t.requires_auth() {
                    if let Some(auth) = params.auth.as_ref() {
                        let sendable_message = SendableMessage::Login(
                            auth.to_login_msg().expect("Failed to get login msg"),
                        );
                        trace!("Sending subscription msg: {:?}", sendable_message);
                        tx.send(sendable_message).await?;
                    }
                }

                // todo: in authenticated requests we don't have to subscribe right after logging in
                let sendable_message = SendableMessage::Subscribe(x);
                trace!("Sending subscription msg: {:?}", sendable_message);
                tx.send(sendable_message).await?;
            }

            let mut topics_lock = acquired_ws.websocket_entry.topics.write().await;
            let topics_websocket_entry_does_not_have = acquired_ws
                .for_topics
                .into_iter()
                .filter(|topic| !topics_lock.contains(topic))
                .collect::<Vec<Topic>>();
            topics_lock.extend(topics_websocket_entry_does_not_have);
        }

        Ok(SubscribeOutput {})
    }
}
