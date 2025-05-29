use dotenv::dotenv;
use mexc_rs::futures::v1::endpoints::get_depth::{DepthParams, GetDepth};
use mexc_rs::futures::v1::endpoints::get_open_orders::{GetOpenOrders, GetOpenOrdersParams};
use mexc_rs::futures::{
    MexcFuturesApiClient, MexcFuturesApiClientWithAuthentication, MexcFuturesApiEndpoint,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    unsafe {
        std::env::set_var("RUST_LOG", "mexc_rs=debug,futures_get_open_orders=trace");
    }
    tracing_subscriber::fmt::init();

    dotenv().ok();

    let client = MexcFuturesApiClient::new(MexcFuturesApiEndpoint::Base);
    let symbol = "BTC_USDT";
    let open_orders = client
        .get_depth(&DepthParams {
            symbol: symbol.to_string(),
            limit: Some(5),
        })
        .await?;
    tracing::info!("{:#?}", open_orders);

    Ok(())
}
