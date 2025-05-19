use crate::spot::ws::message::kline::KlineIntervalTopic;
use serde::{Deserialize, Serialize};

const DEPTH_TOPIC_METHOD: &str = "sub.depth";
const DEPTH_TOPIC_METHOD_FULL: &str = "sub.depth.full";

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Topic {
    Depth(DepthTopic),
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepthTopic {
    method: String,
    pub param: DepthTopicParams,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepthTopicParams {
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compress: Option<bool>,
}

impl Topic {
    pub fn requires_auth(&self) -> bool {
        match self {
            Topic::Depth(_) => false,
        }
    }

    pub fn to_subscription_msg(&self) -> String {
        match self {
            Topic::Depth(depth_topic) => serde_json::to_string(depth_topic)
                .unwrap()
        }
    }
}

impl DepthTopic {
    pub fn new(symbol: String, limit: Option<u32>, compress: Option<bool>) -> Self {
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
