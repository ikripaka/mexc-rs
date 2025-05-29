use dotenv::dotenv;
use mexc_rs::spot::v3::cancel_order::{CancelOrderEndpoint, CancelOrderParams};
use mexc_rs::spot::v3::depth::{DepthEndpoint, DepthParams};
use mexc_rs::spot::v3::enums::{OrderSide, OrderType};
use mexc_rs::spot::v3::order::{OrderEndpoint, OrderParams};
use mexc_rs::spot::{MexcSpotApiClient, MexcSpotApiClientWithAuthentication, MexcSpotApiEndpoint};
use rust_decimal::Decimal;
use std::str::FromStr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    unsafe {
        std::env::set_var("RUST_LOG", "mexc_rs=debug,spot_depth=trace,trace");
    }
    tracing_subscriber::fmt::init();

    dotenv().ok();

    let client = MexcSpotApiClient::new(MexcSpotApiEndpoint::Base);

    let symbol = "BTCUSDT";
    let limit = Some(10);
    let depth = client.depth(DepthParams { symbol, limit }).await;
    tracing::info!("Results: {:?}", depth);

    Ok(())
}
