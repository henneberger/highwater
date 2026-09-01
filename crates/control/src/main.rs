use std::{env, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Response, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use tokio::{net::TcpListener, sync::RwLock, time::MissedTickBehavior};

#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    data_plane: String,
    username: String,
    password: String,
    overview: Arc<RwLock<Option<Bytes>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let listen = env::var("HIGHWATER_CONTROL_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let state = AppState {
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()?,
        data_plane: env::var("HIGHWATER_DATA_PLANE")
            .unwrap_or_else(|_| "https://highwater-cloud.fly.dev".into())
            .trim_end_matches('/')
            .to_owned(),
        username: env::var("HIGHWATER_CONSOLE_USERNAME").unwrap_or_else(|_| "demo".into()),
        password: env::var("HIGHWATER_CONSOLE_PASSWORD").unwrap_or_else(|_| "demo".into()),
        overview: Arc::new(RwLock::new(None)),
    };
    tokio::spawn(refresh_overview(state.clone()));
    let app = Router::new()
        .route("/health", get(health))
        .route("/console/overview", get(overview).options(preflight))
        .route("/console/workflows/{id}", get(workflow).options(preflight))
        .route("/console/processes/{id}", get(process).options(preflight))
        .with_state(state);
    let listener = TcpListener::bind(&listen)
        .await
        .with_context(|| format!("bind control service to {listen}"))?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn refresh_overview(state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let response = state
            .client
            .get(format!("{}/console/overview", state.data_plane))
            .basic_auth(&state.username, Some(&state.password))
            .send()
            .await;
        let Ok(response) = response else { continue };
        if !response.status().is_success() {
            continue;
        }
        if let Ok(body) = response.bytes().await {
            *state.overview.write().await = Some(body);
        }
    }
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn overview(State(state): State<AppState>, headers: HeaderMap) -> Response<Body> {
    if !authorized(&state, &headers) {
        return response(
            StatusCode::UNAUTHORIZED,
            Bytes::from_static(b"{\"error\":\"unauthorized\"}"),
        );
    }
    match state.overview.read().await.clone() {
        Some(body) => response(StatusCode::OK, body),
        None => response(
            StatusCode::SERVICE_UNAVAILABLE,
            Bytes::from_static(b"{\"error\":\"control projection is starting\"}"),
        ),
    }
}

async fn workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    if !authorized(&state, &headers) {
        return response(
            StatusCode::UNAUTHORIZED,
            Bytes::from_static(b"{\"error\":\"unauthorized\"}"),
        );
    }
    let id = utf8_percent_encode(&id, NON_ALPHANUMERIC);
    match state
        .client
        .get(format!("{}/console/workflows/{id}", state.data_plane))
        .basic_auth(&state.username, Some(&state.password))
        .send()
        .await
    {
        Ok(upstream) => {
            let status = upstream.status();
            let body = upstream.bytes().await.unwrap_or_default();
            response(status, body)
        }
        Err(_) => response(
            StatusCode::SERVICE_UNAVAILABLE,
            Bytes::from_static(b"{\"error\":\"data plane is unavailable\"}"),
        ),
    }
}

async fn process(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    if !authorized(&state, &headers) {
        return response(
            StatusCode::UNAUTHORIZED,
            Bytes::from_static(b"{\"error\":\"unauthorized\"}"),
        );
    }
    let id = utf8_percent_encode(&id, NON_ALPHANUMERIC);
    match state
        .client
        .get(format!("{}/console/processes/{id}", state.data_plane))
        .basic_auth(&state.username, Some(&state.password))
        .send()
        .await
    {
        Ok(upstream) => {
            let status = upstream.status();
            let body = upstream.bytes().await.unwrap_or_default();
            response(status, body)
        }
        Err(_) => response(
            StatusCode::SERVICE_UNAVAILABLE,
            Bytes::from_static(b"{\"error\":\"data plane is unavailable\"}"),
        ),
    }
}

async fn preflight() -> Response<Body> {
    response(StatusCode::NO_CONTENT, Bytes::new())
}

fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
    let expected = format!(
        "Basic {}",
        STANDARD.encode(format!("{}:{}", state.username, state.password))
    );
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected)
}

fn response(status: StatusCode, body: Bytes) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("https://demo.highwater.cloud"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("authorization,content-type"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET,OPTIONS"),
    );
    headers.insert(header::VARY, HeaderValue::from_static("Origin"));
    response
}
