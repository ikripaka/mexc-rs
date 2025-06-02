use crate::spot::v3::enums::{OrderSide, OrderType};
use crate::spot::v3::{ApiResponse, ApiResult};
use crate::spot::MexcSpotApiClientWithAuthentication;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use thiserror::Error;
use tracing::info;

pub const DEFAULT_PLACE_ORDER_RECV_WINDOW: u64 = 7000;
pub const MAX_BATCH_ORDER_LIMIT: usize = 20;

#[derive(Debug)]
pub struct OrderParams<'a> {
    pub symbol: &'a str,
    pub recv_window: Option<u64>,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: Option<Decimal>,
    pub quote_order_quantity: Option<Decimal>,
    pub price: Option<Decimal>,
    pub new_client_order_id: Option<&'a str>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderQuery<'a> {
    pub symbol: &'a str,
    pub side: OrderSide,
    #[serde(rename = "type")]
    pub order_type: OrderType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantity: Option<Decimal>,
    #[serde(rename = "quoteOrderQty", skip_serializing_if = "Option::is_none")]
    pub quote_order_quantity: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_client_order_id: Option<&'a str>,
    /// Max 60000
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recv_window: Option<u64>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}

impl<'a> From<OrderParams<'a>> for OrderQuery<'a> {
    fn from(params: OrderParams<'a>) -> Self {
        Self {
            symbol: params.symbol,
            side: params.side,
            order_type: params.order_type,
            quantity: params.quantity,
            quote_order_quantity: params.quote_order_quantity,
            price: params.price,
            new_client_order_id: params.new_client_order_id,
            recv_window: params.recv_window,
            timestamp: Utc::now(),
        }
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBatchQuery {
    batch_orders: String,
    /// Max 60000
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recv_window: Option<u64>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}

impl<'a> TryFrom<BatchParams<'a>> for OrderBatchQuery {
    type Error = BatchOrderError;

    fn try_from(value: BatchParams<'a>) -> Result<Self, Self::Error> {
        if value.orders.len() > MAX_BATCH_ORDER_LIMIT {
            Err(BatchOrderError::ExceedMaxOrderLimit(value.orders.len()))
        } else {
            Ok(OrderBatchQuery {
                batch_orders: serde_json::to_string(
                    &value
                        .orders
                        .into_iter()
                        .map(|x| x.into())
                        .collect::<Vec<OrderQuery>>(),
                )?,
                recv_window: value.recv_window,
                timestamp: Utc::now(),
            })
        }
    }
}

#[derive(Error, Debug)]
pub enum BatchOrderError {
    #[error("Order amount exceed maximum limit: {MAX_BATCH_ORDER_LIMIT}, got: {0}")]
    ExceedMaxOrderLimit(usize),
    #[error("Failed to encode json file to json, err: {0}")]
    FailedToEncode(#[from] serde_json::Error),
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderOutput {
    pub symbol: String,
    pub order_id: String,
    pub order_list_id: Option<i32>,
    pub price: Decimal,
    pub orig_qty: Decimal,
    #[serde(rename = "type")]
    pub order_type: OrderType,
    pub side: OrderSide,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub transact_time: DateTime<Utc>,
}

#[derive(Debug)]
pub struct BatchParams<'a> {
    pub orders: Vec<OrderParams<'a>>,
    pub recv_window: Option<u64>,
}

#[async_trait]
pub trait OrderEndpoint {
    async fn order(&self, params: OrderParams<'_>) -> ApiResult<OrderOutput>;
}

#[async_trait]
impl OrderEndpoint for MexcSpotApiClientWithAuthentication {
    async fn order(&self, params: OrderParams<'_>) -> ApiResult<OrderOutput> {
        let endpoint = format!("{}/api/v3/order", self.endpoint.as_ref());
        let query = OrderQuery::from(params);
        let query_with_signature = self.sign_query(query)?;

        let response = self
            .reqwest_client
            .post(&endpoint)
            .query(&query_with_signature)
            .send()
            .await?;
        let api_response = response.json::<ApiResponse<OrderOutput>>().await?;
        let output = api_response.into_api_result()?;
        Ok(output)
    }
}

#[async_trait]
pub trait OrderBatchEndpoint {
    async fn order_batch(&self, params: BatchParams<'_>) -> ApiResult<Vec<OrderOutput>>;
}

#[async_trait]
impl OrderBatchEndpoint for MexcSpotApiClientWithAuthentication {
    async fn order_batch(&self, params: BatchParams<'_>) -> ApiResult<Vec<OrderOutput>> {
        let endpoint = format!("{}/api/v3/batchOrders", self.endpoint.as_ref());
        let query = OrderBatchQuery::try_from(params)?;
        info!("{query:?}, {:?}", serde_json::to_string(&query));
        let query_with_signature = self.sign_query(&query)?;

        let response = self
            .reqwest_client
            .post(&endpoint)
            .query(&query_with_signature)
            .send()
            .await?;
        let api_response = response.json::<ApiResponse<Vec<OrderOutput>>>().await?;
        let output = api_response.into_api_result()?;
        // let api_response = response.json::<Vec<ApiResponse<OrderOutput>>>().await?;
        // let output = api_response.into_iter().map(|x|x.into_api_result()).collect::<Result<Vec<OrderOutput>, ApiError>>();
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_order() {
        // Fails on insufficient balance
        let client = MexcSpotApiClientWithAuthentication::new_for_test();
        let params = OrderParams {
            symbol: "KASUSDT",
            recv_window: Some(DEFAULT_PLACE_ORDER_RECV_WINDOW),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            quantity: Some(Decimal::from(5000)),
            quote_order_quantity: None,
            price: Some(Decimal::from_str("0.001").unwrap()),
            new_client_order_id: None,
        };
        let result = client.order(params).await;
        assert!(result.is_ok());
    }
}
