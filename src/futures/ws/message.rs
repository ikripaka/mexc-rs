use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use std::convert::TryFrom;

#[derive(Debug)]
pub enum Message {
    Depth(AccountDepthMsg),
    Notification(NotificationMsg),
}

#[derive(Debug, Clone)]
pub struct AccountDepthMsg {
    pub asks: Vec<PriceQuantity>,
    pub bids: Vec<PriceQuantity>,
    pub begin: u64,
    pub end: u64,
    pub version: u64,
}

#[derive(Debug, Clone)]
pub struct NotificationMsg {
    pub channel: String,
    pub data: String,
}

#[derive(Debug, serde::Deserialize)]
#[allow(clippy::large_enum_variant, dead_code)]
#[serde(untagged)]
pub(crate) enum RawMessage {
    ChannelMessage(RawChannelMsg),
    ChannelNotification(RawNotificationMsg),
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct RawChannelMsg {
    pub symbol: String,
    pub data: RawChannelMessageData,
    pub channel: String,
    #[serde(rename = "ts", with = "chrono::serde::ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct RawNotificationMsg {
    pub channel: String,
    pub data: String,
    #[serde(rename = "ts", with = "chrono::serde::ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
pub(crate) enum RawChannelMessageData {
    DepthEvent(RawEventChannelMsgData),
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
#[allow(dead_code)]
pub(crate) enum RawEventChannelMsgData {
    Depth {
        asks: Vec<PriceQuantityEnum>,
        bids: Vec<PriceQuantityEnum>,
        end: u64,
        begin: u64,
        version: u64,
    },
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
pub enum PriceQuantityEnum {
    PriceQuantity(Decimal, Decimal),
    PriceQuantityOrders(Decimal, Decimal, u64),
}

#[derive(Debug, Clone)]
pub struct PriceQuantity {
    pub price: Decimal,
    pub quantity: u64,
}

impl TryFrom<&RawMessage> for Message {
    type Error = ();

    fn try_from(value: &RawMessage) -> Result<Self, Self::Error> {
        match value {
            RawMessage::ChannelMessage(raw_channel_message) => match &raw_channel_message.data {
                RawChannelMessageData::DepthEvent(x) => Ok(Message::from(x)),
            },
            RawMessage::ChannelNotification(x) => Ok(Message::from(x)),
        }
    }
}

impl From<&RawEventChannelMsgData> for Message {
    fn from(value: &RawEventChannelMsgData) -> Self {
        match value {
            RawEventChannelMsgData::Depth {
                asks,
                bids,
                version,
                end,
                begin,
            } => {
                let asks = asks.iter().map(|x| PriceQuantity::from(x)).collect();
                let bids = bids.iter().map(|x| PriceQuantity::from(x)).collect();
                Message::Depth(AccountDepthMsg {
                    asks,
                    bids,
                    begin: *begin,
                    end: *end,
                    version: *version,
                })
            }
        }
    }
}

impl From<RawNotificationMsg> for Message {
    fn from(value: RawNotificationMsg) -> Self {
        Message::Notification(NotificationMsg {
            channel: value.channel,
            data: value.data,
        })
    }
}

impl From<&RawNotificationMsg> for Message {
    fn from(value: &RawNotificationMsg) -> Self {
        Message::Notification(NotificationMsg {
            channel: value.channel.clone(),
            data: value.data.clone(),
        })
    }
}

impl From<PriceQuantityEnum> for PriceQuantity {
    fn from(value: PriceQuantityEnum) -> Self {
        match value {
            PriceQuantityEnum::PriceQuantity(price, quantity) => PriceQuantity {
                price,
                quantity: quantity.floor().to_u64().unwrap(),
            },
            PriceQuantityEnum::PriceQuantityOrders(price, quantity, order) => PriceQuantity {
                price,
                quantity: quantity.floor().to_u64().unwrap() * order,
            },
        }
    }
}

impl From<&PriceQuantityEnum> for PriceQuantity {
    fn from(value: &PriceQuantityEnum) -> Self {
        match value {
            PriceQuantityEnum::PriceQuantity(price, quantity) => PriceQuantity {
                price: *price,
                quantity: quantity.floor().to_u64().unwrap(),
            },
            PriceQuantityEnum::PriceQuantityOrders(price, quantity, order) => PriceQuantity {
                price: *price,
                quantity: quantity.floor().to_u64().unwrap() * order,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_message_depth() {
        let json = r#"{"symbol":"BTC_USDT","data":{"asks":[[103116.1,89918,2]],"bids":[[103115.4,99835,2],[103115.5,100190,2],[9.75E+4,25329.000000000000000000000000000000,58]],"end":25133684088,"begin":25133684085,"version":25133684088},"channel":"push.depth","ts":1747645131820}"#;
        let deserializer = &mut serde_json::Deserializer::from_str(json);

        let result: Result<RawChannelMsg, _> = serde_path_to_error::deserialize(deserializer);
        eprintln!("{:?}", result);
        assert!(result.is_ok());
    }
}
