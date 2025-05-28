use dotenv::dotenv;
use mexc_rs::futures::v1::endpoints::order::{Order, OrderParams};
use mexc_rs::futures::v1::endpoints::ticker::{GetTicker, TickerParams};
use mexc_rs::futures::v1::models::{OpenType, OrderSide, OrderType};
use mexc_rs::futures::{MexcFuturesApiClientWithAuthentication, MexcFuturesApiEndpoint};
use num_traits::FromPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "trace");
    std::env::set_var("MEXC_API_KEY", "mx0vglMNhoefH3uXEj");
    std::env::set_var("MEXC_SECRET_KEY", "5df7ced2694b42fd9ec15e8928bea38f");
    tracing_subscriber::fmt::init();

    dotenv().ok();
    let api_key = std::env::var("MEXC_API_KEY").expect("MEXC_API_KEY not set");
    let secret_key = std::env::var("MEXC_SECRET_KEY").expect("MEXC_SECRET_KEY not set");

    let client = MexcFuturesApiClientWithAuthentication::new(
        MexcFuturesApiEndpoint::Base,
        api_key,
        secret_key,
    );
    // let params = OrderParams {
    //     symbol: "KAS_USDT",
    //     price: Decimal::from_str("0.001").unwrap(),
    //     volume: Decimal::from_str("1").unwrap(),
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
    let params = OrderParams {
        symbol: "PEPE_USDT",
        price: Decimal::from_f64(
            client
                .get_ticker(&TickerParams {
                    symbol: Some(&"PEPE_USDT"),
                })
                .await?
                .ask1,
        )
        .unwrap(),
        volume: Decimal::from(10),
        leverage: None,
        side: OrderSide::OpenLong,
        order_type: OrderType::PriceLimitedOrder,
        open_type: OpenType::Isolated,
        position_id: None,
        external_order_id: None,
        stop_loss_price: None,
        take_profit_price: None,
        position_mode: None,
        reduce_only: None,
    };
    let order_output = client.order(params).await?;
    tracing::info!("Order output: {:#?}", order_output);

    Ok(())
}
