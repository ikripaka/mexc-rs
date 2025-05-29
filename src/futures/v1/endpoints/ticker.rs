use crate::futures::response::ApiResponse;
use crate::futures::result::ApiResult;
use crate::futures::ws::message::PriceQuantityEnum;
use crate::futures::{
    MexcFuturesApiClient, MexcFuturesApiClientWithAuthentication, MexcFuturesApiEndpoint,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use url::Url;

#[async_trait]
pub trait GetTicker<'a, S> {
    async fn get_ticker(&self, params: &'a TickerParams<'a, S>) -> ApiResult<Ticker>;
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTicker {
    pub symbol: String,
    pub last_price: f64,
    pub bid1: f64,
    pub ask1: f64,
    pub volume24: f64,
    pub amount24: f64,
    pub hold_vol: f64,
    pub lower24_price: f64,
    pub high24_price: f64,
    pub rise_fall_rate: f64,
    pub rise_fall_value: f64,
    pub index_price: f64,
    pub fair_price: f64,
    pub funding_rate: f64,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Ticker {
    pub symbol: String,
    pub last_price: f64,
    pub bid1: f64,
    pub ask1: f64,
    pub volume24: f64,
    pub amount24: f64,
    pub hold_vol: f64,
    pub lower24_price: f64,
    pub high24_price: f64,
    pub rise_fall_rate: f64,
    pub rise_fall_value: f64,
    pub index_price: f64,
    pub fair_price: f64,
    pub funding_rate: f64,
    pub timestamp: DateTime<Utc>,
}

pub struct TickerParams<'a, S: 'a> {
    pub symbol: Option<&'a S>,
}

impl From<RawTicker> for Ticker {
    fn from(value: RawTicker) -> Self {
        Self {
            symbol: value.symbol,
            last_price: value.last_price,
            bid1: value.bid1,
            ask1: value.ask1,
            volume24: value.volume24,
            amount24: value.amount24,
            hold_vol: value.hold_vol,
            lower24_price: value.lower24_price,
            high24_price: value.high24_price,
            rise_fall_rate: value.rise_fall_rate,
            rise_fall_value: value.rise_fall_value,
            index_price: value.index_price,
            fair_price: value.fair_price,
            funding_rate: value.funding_rate,
            timestamp: Default::default(),
        }
    }
}

async fn default_impl<'a, S: AsRef<str> + 'a>(
    endpoint: &MexcFuturesApiEndpoint,
    reqwest: &Client,
    params: &'a TickerParams<'a, S>,
) -> ApiResult<Ticker> {
    let url = gather_endpoint(endpoint, params);
    let response = reqwest.get(&url).send().await?;
    let api_response = response.json::<ApiResponse<RawTicker>>().await?;
    let result = api_response.into_api_result()?;
    Ok(result.into())
}

#[inline]
fn gather_endpoint<'a, S: AsRef<str>>(
    endpoint: &MexcFuturesApiEndpoint,
    params: &'a TickerParams<'a, S>,
) -> String {
    match params.symbol {
        None => format!("{}/api/v1/contract/ticker", endpoint.as_ref()),
        Some(s) => Url::parse_with_params(
            &format!("{}/api/v1/contract/ticker", endpoint.as_ref()),
            [("symbol", s.as_ref())],
        )
        .unwrap()
        .as_str()
        .to_string(),
    }
}

#[async_trait]
impl<'a, S: AsRef<str> + Sync> GetTicker<'a, S> for MexcFuturesApiClient {
    async fn get_ticker(&self, params: &'a TickerParams<'a, S>) -> ApiResult<Ticker> {
        default_impl(&self.endpoint, &self.reqwest_client, params).await
    }
}

#[async_trait]
impl<'a, S: AsRef<str> + Sync> GetTicker<'a, S> for MexcFuturesApiClientWithAuthentication {
    async fn get_ticker(&self, params: &'a TickerParams<'a, S>) -> ApiResult<Ticker> {
        default_impl(&self.endpoint, &self.reqwest_client, params).await
    }
}
