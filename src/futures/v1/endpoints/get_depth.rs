use crate::futures::response::ApiResponse;
use crate::futures::result::ApiResult;
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

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
pub enum PriceQuantityEnum {
    PriceQuantity(Decimal, Decimal),
    PriceQuantityOrders(Decimal, Decimal, u64),
}

#[derive(Debug, Clone)]
pub struct Depth {
    asks: Vec<PriceQuantity>,
    bids: Vec<PriceQuantity>,
    version: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PriceQuantity {
    pub price: Decimal,
    pub quantity: u64,
}

pub struct DepthParams {
    pub symbol: &'static str,
    pub limit: Option<u64>,
}

impl From<RawDepth> for Depth {
    fn from(value: RawDepth) -> Self {
        Self {
            asks: value.asks.iter().map(|x| x.into()).collect(),
            bids: value.bids.iter().map(|x| x.into()).collect(),
            version: 0,
            timestamp: Default::default(),
        }
    }
}

impl From<PriceQuantityEnum> for PriceQuantity {
    fn from(value: PriceQuantityEnum) -> Self {
        match value {
            PriceQuantityEnum::PriceQuantity(price, quantity) => PriceQuantity {
                price,
                quantity: quantity.to_u64().unwrap(),
            },
            PriceQuantityEnum::PriceQuantityOrders(price, quantity, order) => PriceQuantity {
                price,
                quantity: quantity.to_u64().unwrap() * order,
            },
        }
    }
}

impl From<&PriceQuantityEnum> for PriceQuantity {
    fn from(value: &PriceQuantityEnum) -> Self {
        match value {
            PriceQuantityEnum::PriceQuantity(price, quantity) => PriceQuantity {
                price: *price,
                quantity: quantity.to_u64().unwrap(),
            },
            PriceQuantityEnum::PriceQuantityOrders(price, quantity, order) => PriceQuantity {
                price: *price,
                quantity: quantity.to_u64().unwrap() * order,
            },
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
