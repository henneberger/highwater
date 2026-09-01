use anyhow::{Context, Result, bail};
use reqwest::{Client, Method, StatusCode};
use serde_json::Value;

pub(crate) struct Api {
    address: String,
    api_key: Option<String>,
    client: Client,
}

impl Api {
    pub(crate) fn new(address: String, api_key: Option<String>) -> Self {
        Self {
            address,
            api_key,
            client: Client::new(),
        }
    }

    pub(crate) async fn get(&self, path: &str) -> Result<Value> {
        self.request(Method::GET, path, None).await
    }

    pub(crate) async fn post(&self, path: &str, body: Value) -> Result<Value> {
        self.request(Method::POST, path, Some(body)).await
    }

    async fn request(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.address));
        if let Some(api_key) = &self.api_key {
            request = request.bearer_auth(api_key);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.context("could not reach Highwater")?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .context("could not read Highwater response")?;
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).context("Highwater returned invalid JSON")?
        };
        if !status.is_success() {
            let message = value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("request failed");
            if status == StatusCode::UNAUTHORIZED {
                bail!("authentication failed: {message}");
            }
            bail!("Highwater returned {status}: {message}");
        }
        Ok(value)
    }
}
