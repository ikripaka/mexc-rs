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
    std::env::set_var(
        "RUST_LOG", "trace", // "mexc_rs=debug,futures_get_open_orders=trace"
    );
    tracing_subscriber::fmt::init();

    std::env::set_var("MEXC_API_KEY", "mx0vglMNhoefH3uXEj");
    std::env::set_var("MEXC_SECRET_KEY", "5df7ced2694b42fd9ec15e8928bea38f");
    dotenv().ok();
    let api_key = std::env::var("MEXC_API_KEY").expect("MEXC_API_KEY not set");
    let secret_key = std::env::var("MEXC_SECRET_KEY").expect("MEXC_SECRET_KEY not set");

    let symbol = "BTC_USDT";

    let api_client = MexcFuturesApiClientWithAuthentication::new(
        MexcFuturesApiEndpoint::Base,
        api_key.clone(),
        secret_key.clone(),
    );

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

    // let params = OrderParams {
    //     symbol: "PEPE_USDT",
    //     price: Decimal::from_f64(api_client.get_ticker(&TickerParams{ symbol: Some(&"PEPE_USDT") }).await?.ask1).unwrap(),
    //     volume:Decimal::from(10),
    //     leverage: None,
    //     side: OrderSide::OpenLong,
    //     order_type: OrderType::PriceLimitedOrder,
    //     open_type: OpenType::Isolated,
    //     position_id: None,
    //     external_order_id: None,
    //     stop_loss_price: None,
    //     take_profit_price: None,
    //     position_mode: None,
    //     reduce_only: None,
    // };

    ws_client
        .send_order(SendOrderParams {}, websocket_auth)
        .await?;
    let mut stream = ws_client.stream();
    while let Some(message) = stream.next().await {
        dbg!(&message);
    }

    Ok(())
}

//  category:1,
//         create_time:Utc::now(),
//         deal_avg_price:0.731,
//         deal_vol:1,
//         error_code:0,
//         external_oid:"_m_95bc2b72d3784bce8f9efecbdef9fe35".to_string(),
//         fee_currency:"USDT".to_string(),
//         leverage:0,
//         maker_fee:0,
//         open_type: OpenType::Isolated,
//         order_id:"102067003631907840".to_string(),
//         order_margin:0,
//         order_type: OpenType::Isolated,
//         position_id:1397818,
//         price:0.707,
//         profit:-0.0005,
//         remain_vol:0,
//         side: Side::OpenLong,
//         state:OrderState::Completed,
//         symbol:"CRV_USDT".to_string(),
//         taker_fee:0.00004386,
//         update_time:Utc::now(),
//         used_margin:0,
//         version:2,
//         vol:1
