use anyhow::{Context, Result, bail};
use std::{collections::VecDeque, env};

pub(crate) struct GlobalOptions {
    pub(crate) address: String,
    pub(crate) api_key: Option<String>,
    pub(crate) arguments: Arguments,
}

pub(crate) struct Arguments(VecDeque<String>);

impl Arguments {
    pub(crate) fn new(values: Vec<String>) -> Self {
        Self(values.into())
    }

    pub(crate) fn next(&mut self) -> Option<String> {
        self.0.pop_front()
    }

    pub(crate) fn required(&mut self, option: &str) -> Result<String> {
        self.next()
            .with_context(|| format!("{option} requires a value"))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn into_vec(self) -> Vec<String> {
        self.0.into()
    }
}

pub(crate) fn global(values: Vec<String>) -> Result<GlobalOptions> {
    let mut input = Arguments::new(values);
    let mut output = Vec::new();
    let mut address =
        env::var("HIGHWATER_ADDRESS").unwrap_or_else(|_| "http://127.0.0.1:7233".to_owned());
    let mut api_key = env::var("HIGHWATER_API_KEY").ok();
    while let Some(value) = input.next() {
        match value.as_str() {
            "--address" => address = input.required("--address")?,
            "--api-key" => api_key = Some(input.required("--api-key")?),
            _ => output.push(value),
        }
    }
    if address.trim().is_empty() {
        bail!("--address must not be empty");
    }
    if api_key.as_ref().is_some_and(|value| value.is_empty()) {
        bail!("--api-key must not be empty");
    }
    Ok(GlobalOptions {
        address: address.trim_end_matches('/').to_owned(),
        api_key,
        arguments: Arguments::new(output),
    })
}
