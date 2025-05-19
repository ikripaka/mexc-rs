use crate::futures::ws::message::Message;
use crate::futures::ws::public_futures_ws::MexcFuturesWebsocketClient;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::sync::Arc;

pub trait Stream {
    fn stream<'a>(self: Arc<Self>) -> BoxStream<'a, Arc<Message>>;
}

impl Stream for MexcFuturesWebsocketClient {
    fn stream<'a>(self: Arc<Self>) -> BoxStream<'a, Arc<Message>> {
        let mut rx = self.broadcast_tx.subscribe();
        let stream = async_stream::stream! {
            while let Ok(message) = rx.recv().await {
                yield message;
            }
        };
        stream.boxed()
    }
}
