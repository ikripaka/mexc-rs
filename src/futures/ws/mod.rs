pub mod acquire_websocket;
pub mod endpoint;
pub mod error;
pub mod message;
pub mod public_futures_ws;
pub mod send_order;
pub mod stream;
pub mod subscribe;
pub mod topic;

pub use crate::spot::ws::auth::WebsocketAuth;
