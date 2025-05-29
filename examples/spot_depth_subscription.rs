use dotenv::dotenv;
use futures::StreamExt;
use mexc_rs::spot::ws::auth::WebsocketAuth;
use mexc_rs::spot::ws::message::kline::KlineIntervalTopic;
use mexc_rs::spot::ws::stream::Stream;
use mexc_rs::spot::ws::subscribe::{Subscribe, SubscribeParams};
use mexc_rs::spot::ws::topic::{DealsTopic, DepthTopic, KlineTopic, Topic};
use mexc_rs::spot::ws::MexcSpotWebsocketClient;

#[tokio::main]
async fn main() {
    unsafe {
        std::env::set_var("RUST_LOG", "mexc_rs=debug,spot_depth_subscription=trace");
    }
    tracing_subscriber::fmt::init();

    dotenv().ok();

    let ws_client = MexcSpotWebsocketClient::default().into_arc();
    ws_client
        .clone()
        .subscribe(SubscribeParams::default().with_topics(vec![
            Topic::Depth(DepthTopic {
                symbol: "PEPEUSDT".to_string(),
            }),
            Topic::Depth(DepthTopic {
                symbol: "ADAUSDT".to_string(),
            }),
            Topic::Depth(DepthTopic {
                symbol: "SOLUSDT".to_string(),
            }),
        ]))
        .await
        .expect("Failed to subscribe");

    let mut stream = ws_client.stream();
    while let Some(message) = stream.next().await {
        dbg!(&message);
    }
}
