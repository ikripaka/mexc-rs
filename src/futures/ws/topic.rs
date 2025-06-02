use serde::{Deserialize, Serialize};
use strum::{EnumString, VariantNames};

const DEPTH_TOPIC_METHOD: &str = "sub.depth";
const DEPTH_TOPIC_METHOD_FULL: &str = "sub.depth.full";
const ORDER_TOPIC_METHOD: &str = "sub.personal.order";

#[derive(Debug, Clone, Hash, PartialEq, Eq, EnumString, VariantNames)]
pub enum Topic {
    Depth(DepthTopic),
    Order(OrderTopic),
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DepthTopic {
    method: String,
    pub param: DepthTopicParams,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OrderTopic {
    method: String,
    pub param: OrderTopicParams,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DepthTopicParams {
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compress: Option<bool>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OrderTopicParams {
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compress: Option<bool>,
}

impl Topic {
    pub fn requires_auth(&self) -> bool {
        match self {
            Topic::Depth(_) => false,
            &Topic::Order(_) => true,
        }
    }

    pub fn to_subscription_msg(&self) -> String {
        match self {
            Topic::Depth(depth_topic) => serde_json::to_string(depth_topic).unwrap(),
            Topic::Order(order_topic) => serde_json::to_string(order_topic).unwrap(),
        }
    }
}

impl DepthTopic {
    pub fn new(symbol: String, limit: Option<u64>, compress: Option<bool>) -> Self {
        Self {
            method: if limit.is_some() {
                DEPTH_TOPIC_METHOD_FULL.to_string()
            } else {
                DEPTH_TOPIC_METHOD.to_string()
            },
            param: DepthTopicParams {
                symbol,
                limit,
                compress,
            },
        }
    }
}

impl OrderTopic {
    pub fn new() -> Self {
        Self {
            method: "".to_string(),
            param: Default::default(),
        }
    }
}
