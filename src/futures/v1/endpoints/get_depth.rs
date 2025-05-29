use crate::futures::response::ApiResponse;
use crate::futures::result::ApiResult;
use crate::futures::ws::message::{PriceQuantityEnum, PriceQuantityFut};
use crate::futures::{
    MexcFuturesApiClient, MexcFuturesApiClientWithAuthentication, MexcFuturesApiEndpoint,
};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use num_traits::ToPrimitive;
use reqwest::Client;
use rust_decimal::Decimal;
use tracing::info;

#[async_trait]
pub trait GetDepth {
    async fn get_depth(&self, params: &DepthParams) -> ApiResult<Depth>;
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawDepth {
    asks: Vec<PriceQuantityEnum>,
    bids: Vec<PriceQuantityEnum>,
    version: u64,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Depth {
    pub asks: Vec<PriceQuantityFut>,
    pub bids: Vec<PriceQuantityFut>,
    pub version: u64,
    pub timestamp: DateTime<Utc>,
}

pub struct DepthParams {
    pub symbol: String,
    pub limit: Option<u64>,
}

impl From<RawDepth> for Depth {
    fn from(value: RawDepth) -> Self {
        Self {
            asks: value.asks.iter().map(|x| x.into()).collect(),
            bids: value.bids.iter().map(|x| x.into()).collect(),
            version: value.version,
            timestamp: Default::default(),
        }
    }
}

async fn default_impl(
    endpoint: &MexcFuturesApiEndpoint,
    reqwest: &Client,
    params: &DepthParams,
) -> ApiResult<Depth> {
    let url = gather_endpoint(endpoint, params);
    let response = reqwest.get(&url).send().await?;
    let api_response = response.json::<ApiResponse<RawDepth>>().await?;
    let result = api_response.into_api_result()?;
    Ok(result.into())
}

#[inline]
fn gather_endpoint(endpoint: &MexcFuturesApiEndpoint, params: &DepthParams) -> String {
    match params.limit {
        None => format!(
            "{}/api/v1/contract/depth/{}",
            endpoint.as_ref(),
            params.symbol
        ),
        Some(limit) => format!(
            "{}/api/v1/contract/depth/{}?limit={}",
            endpoint.as_ref(),
            params.symbol,
            limit
        ),
    }
}

#[async_trait]
impl GetDepth for MexcFuturesApiClient {
    async fn get_depth(&self, params: &DepthParams) -> ApiResult<Depth> {
        default_impl(&self.endpoint, &self.reqwest_client, params).await
    }
}

#[async_trait]
impl GetDepth for MexcFuturesApiClientWithAuthentication {
    async fn get_depth(&self, params: &DepthParams) -> ApiResult<Depth> {
        default_impl(&self.endpoint, &self.reqwest_client, params).await
    }
}
