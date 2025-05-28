use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::Sha256;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct WebsocketAuth {
    pub api_key: String,
    pub secret_key: String,
}

impl WebsocketAuth {
    pub fn new(api_key: String, secret_key: String) -> Self {
        Self {
            api_key,
            secret_key,
        }
    }

    pub(crate) fn to_login_msg(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(&LoginMsg {
            method: "login".to_string(),
            param: LoginParams::new(&self, Utc::now().timestamp_subsec_millis().to_string())?,
        })?)
    }
}

#[derive(Debug, serde::Serialize)]
struct LoginMsg {
    pub method: String,
    pub param: LoginParams,
}

#[derive(serde::Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct LoginParams {
    pub api_key: String,
    pub req_time: String,
    pub signature: String,
}

impl LoginParams {
    fn new(auth: &WebsocketAuth, req_time: String) -> anyhow::Result<Self> {
        let query_string = format!("{}{req_time}", auth.api_key);
        let mut mac = Hmac::<Sha256>::new_from_slice(auth.secret_key.as_bytes())?;
        mac.update(query_string.as_bytes());
        let mac_result = mac.finalize();
        let mac_bytes = mac_result.into_bytes();
        let signature = hex::encode(mac_bytes);

        Ok(LoginParams {
            api_key: auth.api_key.to_string(),
            req_time,
            signature,
        })
    }
}
