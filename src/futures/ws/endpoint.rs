use std::fmt;

#[derive(Debug)]
pub enum MexcFuturesWebsocketEndpoint {
    Base,
    Custom(String),
}

impl AsRef<str> for MexcFuturesWebsocketEndpoint {
    fn as_ref(&self) -> &str {
        match self {
            MexcFuturesWebsocketEndpoint::Base => "wss://contract.mexc.com/edge",
            MexcFuturesWebsocketEndpoint::Custom(endpoint) => endpoint,
        }
    }
}

impl fmt::Display for MexcFuturesWebsocketEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}
