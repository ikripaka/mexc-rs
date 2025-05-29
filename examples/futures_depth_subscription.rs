use dotenv::dotenv;
use futures::StreamExt;
use mexc_rs::futures::ws::public_futures_ws::MexcFuturesWebsocketClient;
use mexc_rs::futures::ws::stream::Stream;
use mexc_rs::futures::ws::subscribe::{Subscribe, SubscribeParams};
use mexc_rs::futures::ws::topic::{DepthTopic, Topic};

#[tokio::main]
async fn main() {
    unsafe {
        std::env::set_var(
            "RUST_LOG",
            "mexc_rs=debug,spot_simple_private_subscription=trace",
        );
    }
    tracing_subscriber::fmt::init();

    let ws_client = MexcFuturesWebsocketClient::default().into_arc();
    ws_client
        .clone()
        .subscribe(
            SubscribeParams::default().with_topic(Topic::Depth(DepthTopic::new(
                "BTC_USDT".to_string(),
                None,
                None,
            ))),
        )
        .await
        .expect("Failed to subscribe");

    let mut stream = ws_client.stream();
    while let Some(message) = stream.next().await {
        dbg!(&message);
    }
}
