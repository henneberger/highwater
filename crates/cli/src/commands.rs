use crate::{
    api::Api,
    arguments::{Arguments, global},
};
use anyhow::{Context, Result, bail};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::{Map, Value, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const HELP: &str = r#"Highwater durable streaming

Usage:
  highwater server start-dev [server options]
  highwater workflow start --type TYPE [--id ID] [--arg JSON] [--wait]
  highwater workflow describe --id ID
  highwater workflow history --id ID
  highwater workflow signal --id ID --name NAME [--arg JSON]
  highwater process describe --id ID
  highwater process state --id ID --key KEY
  highwater process send --id ID --key KEY --event JSON [--event-time UNIX] [--event-id ID]
  highwater process complete --id ID --through UNIX
  highwater example run account-balance|order|temporal-order

Global options:
  --address URL   Highwater endpoint (or HIGHWATER_ADDRESS)
  --api-key KEY   Bearer credential (or HIGHWATER_API_KEY)
"#;

pub(crate) async fn run(values: Vec<String>) -> Result<()> {
    let mut options = global(values)?;
    let Some(command) = options.arguments.next() else {
        print!("{HELP}");
        return Ok(());
    };
    if matches!(command.as_str(), "help" | "--help" | "-h") {
        print!("{HELP}");
        return Ok(());
    }
    if matches!(command.as_str(), "version" | "--version" | "-V") {
        println!("highwater {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if command == "server" {
        return server(options.arguments).await;
    }
    let api = Api::new(options.address, options.api_key);
    match command.as_str() {
        "workflow" => workflow(&api, options.arguments).await,
        "process" => process(&api, options.arguments).await,
        "example" => example(&api, options.arguments).await,
        _ => bail!("unknown command {command:?}\n\n{HELP}"),
    }
}

async fn server(mut arguments: Arguments) -> Result<()> {
    match arguments.next().as_deref() {
        Some("start-dev") => highwater_server::run_with_args(arguments.into_vec()).await,
        Some(command) => bail!("unknown server command {command:?}"),
        None => bail!("server requires start-dev"),
    }
}

async fn workflow(api: &Api, mut arguments: Arguments) -> Result<()> {
    match arguments.next().as_deref() {
        Some("start") => workflow_start(api, arguments).await,
        Some("describe") => {
            let id = required_option(arguments, "--id")?;
            print_json(&api.get(&format!("/workflows/{}", encoded(&id))).await?)
        }
        Some("history") => {
            let id = required_option(arguments, "--id")?;
            print_json(
                &api.get(&format!("/workflows/{}/history", encoded(&id)))
                    .await?,
            )
        }
        Some("signal") => workflow_signal(api, arguments).await,
        Some(command) => bail!("unknown workflow command {command:?}"),
        None => bail!("workflow requires start, describe, history, or signal"),
    }
}

async fn workflow_start(api: &Api, mut arguments: Arguments) -> Result<()> {
    let mut workflow_type = None;
    let mut workflow_id = None;
    let mut task_queue = "default".to_owned();
    let mut inputs = Vec::new();
    let mut wait = false;
    while let Some(option) = arguments.next() {
        match option.as_str() {
            "--type" => workflow_type = Some(arguments.required("--type")?),
            "--id" => workflow_id = Some(arguments.required("--id")?),
            "--task-queue" => task_queue = arguments.required("--task-queue")?,
            "--arg" => inputs.push(json_argument(&arguments.required("--arg")?)?),
            "--input" => {
                let value = json_argument(&arguments.required("--input")?)?;
                inputs = value
                    .as_array()
                    .cloned()
                    .context("--input must be a JSON array")?;
            }
            "--wait" => wait = true,
            _ => bail!("unknown workflow start option {option:?}"),
        }
    }
    let response = api
        .post(
            "/workflows",
            json!({
                "workflow_type": workflow_type.context("--type is required")?,
                "workflow_id": workflow_id,
                "args": inputs,
                "options": {"task_queue": task_queue},
            }),
        )
        .await?;
    if wait {
        let id = response["workflow_id"]
            .as_str()
            .context("start response did not include workflow_id")?;
        print_json(&wait_for_workflow(api, id, Duration::from_secs(60)).await?)
    } else {
        print_json(&response)
    }
}

async fn workflow_signal(api: &Api, mut arguments: Arguments) -> Result<()> {
    let mut id = None;
    let mut name = None;
    let mut inputs = Vec::new();
    while let Some(option) = arguments.next() {
        match option.as_str() {
            "--id" => id = Some(arguments.required("--id")?),
            "--name" => name = Some(arguments.required("--name")?),
            "--arg" => inputs.push(json_argument(&arguments.required("--arg")?)?),
            _ => bail!("unknown workflow signal option {option:?}"),
        }
    }
    let id = id.context("--id is required")?;
    let name = name.context("--name is required")?;
    print_json(
        &api.post(
            &format!("/workflows/{}/signals/{}", encoded(&id), encoded(&name)),
            json!({"args": inputs}),
        )
        .await?,
    )
}

async fn process(api: &Api, mut arguments: Arguments) -> Result<()> {
    match arguments.next().as_deref() {
        Some("describe") => {
            let id = required_option(arguments, "--id")?;
            print_json(&api.get(&format!("/processes/{}", encoded(&id))).await?)
        }
        Some("state") => {
            let values = named_options(arguments, &["--id", "--key"])?;
            print_json(
                &api.get(&format!(
                    "/processes/{}/keys/{}",
                    encoded(required(&values, "--id")?),
                    encoded(required(&values, "--key")?),
                ))
                .await?,
            )
        }
        Some("send") => process_send(api, arguments).await,
        Some("complete") => {
            let values = named_options(arguments, &["--id", "--through"])?;
            let through = required(&values, "--through")?
                .parse::<f64>()
                .context("--through must be numeric")?;
            print_json(
                &api.post(
                    &format!(
                        "/processes/{}/complete-through",
                        encoded(required(&values, "--id")?)
                    ),
                    json!({"event_time": through}),
                )
                .await?,
            )
        }
        Some(command) => bail!("unknown process command {command:?}"),
        None => bail!("process requires describe, state, send, or complete"),
    }
}

async fn process_send(api: &Api, mut arguments: Arguments) -> Result<()> {
    let mut id = None;
    let mut key = None;
    let mut event = None;
    let mut event_time = None;
    let mut event_id = None;
    while let Some(option) = arguments.next() {
        match option.as_str() {
            "--id" => id = Some(arguments.required("--id")?),
            "--key" => key = Some(arguments.required("--key")?),
            "--event" => event = Some(json_argument(&arguments.required("--event")?)?),
            "--event-time" => {
                event_time = Some(
                    arguments
                        .required("--event-time")?
                        .parse::<f64>()
                        .context("--event-time must be numeric")?,
                )
            }
            "--event-id" => event_id = Some(arguments.required("--event-id")?),
            _ => bail!("unknown process send option {option:?}"),
        }
    }
    let id = id.context("--id is required")?;
    let response = api
        .post(
            &format!("/processes/{}/events", encoded(&id)),
            json!({"records": [{
                "partition": 0,
                "event_time": event_time.unwrap_or_else(now),
                "key": key.context("--key is required")?,
                "value": event.context("--event is required")?,
                "kind": "upsert",
                "event_id": event_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            }]}),
        )
        .await?;
    print_json(&response)
}

async fn example(api: &Api, mut arguments: Arguments) -> Result<()> {
    if arguments.next().as_deref() != Some("run") {
        bail!("example requires run");
    }
    let name = arguments.next().context("example run requires a name")?;
    if !arguments.is_empty() {
        bail!("example run accepts one example name");
    }
    match name.as_str() {
        "account-balance" => account_balance_example(api).await,
        "order" => order_example(api).await,
        "temporal-order" => temporal_order_example(api).await,
        _ => bail!("unknown example {name:?}; use account-balance, order, or temporal-order"),
    }
}

async fn account_balance_example(api: &Api) -> Result<()> {
    let suffix = Uuid::new_v4().simple().to_string();
    let id = format!("account-balances-{}", &suffix[..8]);
    let stream = format!("{id}-input");
    api.post(
        "/streams",
        json!({
            "name": stream,
            "partitions": 1,
            "watermark_mode": "bounded",
            "max_out_of_orderness": 5.0,
            "idle_timeout": 60.0,
            "allowed_lateness": 0.0,
            "late_policy": "side_output",
        }),
    )
    .await?;
    api.post(
        "/processes",
        json!({
            "process_id": id,
            "stream": stream,
            "workflow_type": "AccountBalanceProcess",
            "key_field": "account_id",
            "event_time_field": "occurred_at",
            "state_version": 1,
            "build_id": "account-balance-v1",
            "migrations_from": [],
            "task_queue": "default",
            "event_time_gate": "complete",
            "max_concurrent_keys": 64,
            "mailbox_capacity": 10000,
            "batch_max_size": 64,
            "batch_max_delay": 0.005,
        }),
    )
    .await?;
    let records = [
        ("account-a", 5, 10.0),
        ("account-a", 7, 12.0),
        ("account-b", 3, 11.0),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (key, amount, occurred_at))| {
        json!({
                "partition": 0,
                "event_time": occurred_at,
                "key": key,
                "value": {"account_id": key, "amount": amount, "occurred_at": occurred_at},
                "kind": "upsert",
                "event_id": format!("{id}:{index}"),
        })
    })
    .collect::<Vec<_>>();
    api.post(
        &format!("/streams/{}/records/batch", encoded(&stream)),
        json!({"records": records}),
    )
    .await?;
    api.post(
        &format!("/streams/{}/partitions/0/seal", encoded(&stream)),
        json!({}),
    )
    .await?;
    let account_a = wait_for_state(api, &id, "account-a", Duration::from_secs(30)).await?;
    let account_b = wait_for_state(api, &id, "account-b", Duration::from_secs(30)).await?;
    print_json(&json!({"process_id": id, "account_a": account_a, "account_b": account_b}))
}

async fn order_example(api: &Api) -> Result<()> {
    let id = format!("order-{}", &Uuid::new_v4().simple().to_string()[..8]);
    api.post(
        "/workflows",
        json!({
            "workflow_type": "OrderWorkflow",
            "workflow_id": id,
            "args": ["4242424242424242", 25],
            "options": {"task_queue": "default"},
        }),
    )
    .await?;
    api.post(
        &format!("/workflows/{}/signals/approve", encoded(&id)),
        json!({"args": []}),
    )
    .await?;
    print_json(&wait_for_workflow(api, &id, Duration::from_secs(30)).await?)
}

async fn temporal_order_example(api: &Api) -> Result<()> {
    let suffix = &Uuid::new_v4().simple().to_string()[..8];
    let profiles = format!("demo-customer-profiles-{suffix}");
    let orders = format!("demo-ready-orders-{suffix}");
    let join_id = format!("demo-orders-at-profile-{suffix}");
    let customer_id = format!("customer-{suffix}");
    let order_id = format!("order-{suffix}");
    for stream in [&profiles, &orders] {
        api.post(
            "/streams",
            json!({
                "name": stream,
                "partitions": 1,
                "watermark_mode": "source_managed",
                "max_out_of_orderness": 0.0,
                "idle_timeout": 60.0,
                "allowed_lateness": 0.0,
                "late_policy": "side_output",
            }),
        )
        .await?;
    }
    api.post(
        "/temporal-joins",
        json!({
            "join_id": join_id,
            "probe_stream": orders,
            "version_stream": profiles,
            "workflow_type": "FulfillReadyOrder",
            "task_queue": "orders",
            "join_type": "left",
        }),
    )
    .await?;
    api.post(
        &format!("/streams/{}/records/batch", encoded(&profiles)),
        json!({"records": [
            {
                "partition": 0, "event_time": 0.0, "key": customer_id,
                "value": {"version": "standard-v1", "active": true, "tier": "standard", "order_limit": 5000},
                "kind": "upsert", "event_id": format!("{customer_id}:v1"),
            },
            {
                "partition": 0, "event_time": 4.0, "key": customer_id,
                "value": {"version": "premium-v2", "active": true, "tier": "premium", "order_limit": 10000},
                "kind": "upsert", "event_id": format!("{customer_id}:v2"),
            }
        ]}),
    )
    .await?;
    api.post(
        &format!("/streams/{}/records", encoded(&orders)),
        json!({
            "partition": 0,
            "event_time": 3.0,
            "key": customer_id,
            "value": {
                "status": "ready", "order_id": order_id, "customer_id": customer_id,
                "lines": [{"sku": "coffee", "quantity": 2, "unit_price": 1200}, {"sku": "filters", "quantity": 1, "unit_price": 800}],
                "total": 3200, "payment_reference": "payment-demo-4242", "address": "1 Highwater Way",
            },
            "kind": "upsert",
            "event_id": format!("{order_id}:ready"),
        }),
    )
    .await?;
    for stream in [&profiles, &orders] {
        api.post(
            &format!("/streams/{}/partitions/0/watermark", encoded(stream)),
            json!({"event_time": 10.0}),
        )
        .await?;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let workflow_id = loop {
        let outputs = api
            .get(&format!("/temporal-joins/{}/outputs", encoded(&join_id)))
            .await?;
        if let Some(id) = outputs
            .as_array()
            .and_then(|values| values.first())
            .and_then(|value| value["workflow_id"].as_str())
        {
            break id.to_owned();
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("temporal order example did not emit a joined order");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let workflow = wait_for_workflow(api, &workflow_id, Duration::from_secs(30)).await?;
    print_json(&json!({
        "temporal_join": join_id,
        "as_of": 3.0,
        "selected_profile": workflow["result"]["customer_version"],
        "later_profile": "premium-v2",
        "workflow": workflow,
    }))
}

async fn wait_for_workflow(api: &Api, id: &str, timeout: Duration) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let value = api.get(&format!("/workflows/{}", encoded(id))).await?;
        if value["status"] != "RUNNING" {
            return Ok(value);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "workflow {id} did not complete within {} seconds",
                timeout.as_secs()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_state(api: &Api, id: &str, key: &str, timeout: Duration) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let value = api
            .get(&format!("/processes/{}/keys/{}", encoded(id), encoded(key)))
            .await?;
        if !value["state"].is_null() {
            return Ok(value["state"].clone());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("process {id} did not produce state for {key}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn named_options(mut arguments: Arguments, allowed: &[&str]) -> Result<Map<String, Value>> {
    let mut values = Map::new();
    while let Some(option) = arguments.next() {
        if !allowed.contains(&option.as_str()) {
            bail!("unknown option {option:?}");
        }
        values.insert(option.clone(), Value::String(arguments.required(&option)?));
    }
    Ok(values)
}

fn required_option(arguments: Arguments, name: &str) -> Result<String> {
    let values = named_options(arguments, &[name])?;
    Ok(required(&values, name)?.to_owned())
}

fn required<'a>(values: &'a Map<String, Value>, name: &str) -> Result<&'a str> {
    values
        .get(name)
        .and_then(Value::as_str)
        .with_context(|| format!("{name} is required"))
}

fn json_argument(value: &str) -> Result<Value> {
    serde_json::from_str(value).with_context(|| format!("invalid JSON argument {value:?}"))
}

fn encoded(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn print_json(value: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_named_options_without_accepting_unknown_flags() {
        let values = named_options(
            Arguments::new(vec!["--id".to_owned(), "one".to_owned()]),
            &["--id"],
        )
        .unwrap();
        assert_eq!(required(&values, "--id").unwrap(), "one");
        assert!(named_options(Arguments::new(vec!["--other".to_owned()]), &["--id"]).is_err());
    }
}
