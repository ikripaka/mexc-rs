use chrono::Utc;
use dotenv::dotenv;
use futures::StreamExt;
use mexc_rs::futures::v1::endpoints::get_depth::{DepthParams, GetDepth};
use mexc_rs::futures::v1::endpoints::get_open_orders::{GetOpenOrders, GetOpenOrdersParams};
use mexc_rs::futures::v1::endpoints::ticker::{GetTicker, TickerParams};
use mexc_rs::futures::v1::models::OrderSide;
use mexc_rs::futures::ws::public_futures_ws::MexcFuturesWebsocketClient;
use mexc_rs::futures::ws::send_order::{
    OpenType, OrderState, OrderType, SendOrder, SendOrderParams, Side,
};
use mexc_rs::futures::ws::stream::Stream;
use mexc_rs::futures::ws::subscribe::{Subscribe, SubscribeParams};
use mexc_rs::futures::ws::topic::{DepthTopic, OrderTopic, Topic};
use mexc_rs::futures::ws::WebsocketAuth;
use mexc_rs::futures::{
    MexcFuturesApiClient, MexcFuturesApiClientWithAuthentication, MexcFuturesApiEndpoint,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    unsafe {
        std::env::set_var(
            "RUST_LOG",
            "mexc_rs=debug,futures_ws_authenticated_order=trace",
        );
    }
    tracing_subscriber::fmt::init();

    dotenv().ok();
    let api_key = std::env::var("MEXC_API_KEY").expect("MEXC_API_KEY not set");
    let secret_key = std::env::var("MEXC_SECRET_KEY").expect("MEXC_SECRET_KEY not set");

    let websocket_auth = WebsocketAuth::new(api_key, secret_key);
    let ws_client = MexcFuturesWebsocketClient::default().into_arc();
    ws_client
        .clone()
        .subscribe(
            SubscribeParams::default()
                .with_auth(websocket_auth.clone())
                .with_topic(Topic::Order(OrderTopic::new())),
        )
        .await
        .expect("Failed to subscribe");

    ws_client
        .send_order(
            SendOrderParams {
                symbol: "".to_string(),
                price: 10.0,
                vol: 0,
                side: Side::OpenLong,
                leverage: 0,
                open_type: OpenType::Isolated,
                order_type: OrderType::Limit,
                external_oid: "12345".to_string(),
            },
            websocket_auth,
        )
        .await?;
    let mut stream = ws_client.stream();
    while let Some(message) = stream.next().await {
        dbg!(&message);
    }

    Ok(())
}
