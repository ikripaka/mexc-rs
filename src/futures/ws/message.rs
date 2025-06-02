use chrono::{DateTime, Utc};
use std::convert::TryFrom;

#[derive(Debug)]
pub enum Message {
    DepthWithoutBounds(AccountDepthWithoutBoundsMsg),
    DepthWithBounds(AccountDepthWithBoundsMsg),
    Notification(NotificationMsg),
}

#[derive(Debug, Clone)]
pub struct AccountDepthWithoutBoundsMsg {
    pub asks: Vec<PriceQuantityFut>,
    pub bids: Vec<PriceQuantityFut>,
    pub begin: u64,
    pub end: u64,
    pub version: u64,
}

#[derive(Debug, Clone)]
pub struct AccountDepthWithBoundsMsg {
    pub asks: Vec<PriceQuantityFut>,
    pub bids: Vec<PriceQuantityFut>,
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
    DepthWithoutBounds {
        asks: Vec<PriceQuantityEnum>,
        bids: Vec<PriceQuantityEnum>,
        end: u64,
        begin: u64,
        version: u64,
    },
    DepthWithBounds {
        asks: Vec<PriceQuantityEnum>,
        bids: Vec<PriceQuantityEnum>,
        version: u64,
    },
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
pub enum PriceQuantityEnum {
    PriceQuantity(f64, f64),
    PriceQuantityOrders(f64, f64, u64),
}

#[derive(Debug, Clone)]
pub struct PriceQuantityFut {
    pub price: f64,
    pub quantity: f64,
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
            RawEventChannelMsgData::DepthWithoutBounds {
                asks,
                bids,
                version,
                end,
                begin,
            } => {
                let asks = asks.iter().map(|x| PriceQuantityFut::from(x)).collect();
                let bids = bids.iter().map(|x| PriceQuantityFut::from(x)).collect();
                Message::DepthWithoutBounds(AccountDepthWithoutBoundsMsg {
                    asks,
                    bids,
                    begin: *begin,
                    end: *end,
                    version: *version,
                })
            }
            RawEventChannelMsgData::DepthWithBounds {
                asks,
                bids,
                version,
            } => {
                let asks = asks.iter().map(|x| PriceQuantityFut::from(x)).collect();
                let bids = bids.iter().map(|x| PriceQuantityFut::from(x)).collect();
                Message::DepthWithBounds(AccountDepthWithBoundsMsg {
                    asks,
                    bids,
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

impl From<PriceQuantityEnum> for PriceQuantityFut {
    fn from(value: PriceQuantityEnum) -> Self {
        match value {
            PriceQuantityEnum::PriceQuantity(price, quantity) => {
                PriceQuantityFut { price, quantity }
            }
            PriceQuantityEnum::PriceQuantityOrders(price, quantity, order) => PriceQuantityFut {
                price,
                quantity: quantity * order as f64,
            },
        }
    }
}

impl From<&PriceQuantityEnum> for PriceQuantityFut {
    fn from(value: &PriceQuantityEnum) -> Self {
        match value {
            PriceQuantityEnum::PriceQuantity(price, quantity) => PriceQuantityFut {
                price: *price,
                quantity: *quantity,
            },
            PriceQuantityEnum::PriceQuantityOrders(price, quantity, order) => PriceQuantityFut {
                price: *price,
                quantity: quantity * *order as f64,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_message_depth() {
        let json = r#"{"symbol":"BTC_USDT","data":{"asks":[[105254.2,82164,2],[105254.3,76356,1],[105254.4,70649,1],[105254.5,82216,2],[105254.6,102277,2],[105254.7,79841,1]],"bids":[[105254.1,77560,1],[105254,90450,2]],"version":25158748435},"channel":"push.depth.full","ts":1747759465526}"#;
        let deserializer = &mut serde_json::Deserializer::from_str(json);

        let result: Result<RawChannelMsg, _> = serde_path_to_error::deserialize(deserializer);
        eprintln!("{:?}", result);
        assert!(result.is_ok());
    }
}
