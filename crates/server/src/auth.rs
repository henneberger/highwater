use crate::*;

pub(crate) async fn health() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

pub(crate) async fn cloud_status() -> impl IntoResponse {
    Json(json!({"service": "highwater", "status": "ready"}))
}

pub(crate) async fn browser_cors(request: Request, next: Next) -> Response {
    let preflight = request.method() == axum::http::Method::OPTIONS;
    let origin = request
        .headers()
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .filter(|value| allowed_browser_origin(value))
        .and_then(|_| request.headers().get(axum::http::header::ORIGIN).cloned());
    let mut response = if preflight && origin.is_some() {
        StatusCode::NO_CONTENT.into_response()
    } else {
        next.run(request).await
    };
    if let Some(origin) = origin {
        let headers = response.headers_mut();
        headers.insert(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        headers.insert(
            axum::http::header::ACCESS_CONTROL_ALLOW_METHODS,
            axum::http::HeaderValue::from_static("GET, POST, OPTIONS"),
        );
        headers.insert(
            axum::http::header::ACCESS_CONTROL_ALLOW_HEADERS,
            axum::http::HeaderValue::from_static("Authorization, Content-Type"),
        );
        headers.insert(
            axum::http::header::VARY,
            axum::http::HeaderValue::from_static("Origin"),
        );
    }
    response
}

fn allowed_browser_origin(origin: &str) -> bool {
    origin == "https://highwater.cloud"
        || origin == "http://localhost:8080"
        || origin == "http://127.0.0.1:8080"
}

pub(crate) async fn require_bearer(
    State(expected): State<Arc<String>>,
    request: Request,
    next: Next,
) -> Response {
    let supplied = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if supplied.is_some_and(|value| constant_time_eq(value.as_bytes(), expected.as_bytes())) {
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "missing or invalid API key"})),
    )
        .into_response()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_tokens_require_an_exact_match() {
        assert!(constant_time_eq(b"highwater", b"highwater"));
        assert!(!constant_time_eq(b"highwater", b"highwaters"));
        assert!(!constant_time_eq(b"highwater", b"highwateR"));
    }

    #[test]
    fn cloud_console_is_the_only_production_browser_origin() {
        assert!(allowed_browser_origin("https://highwater.cloud"));
        assert!(allowed_browser_origin("http://localhost:8080"));
        assert!(!allowed_browser_origin("https://api.highwater.cloud"));
        assert!(!allowed_browser_origin(
            "https://highwater.cloud.attacker.test"
        ));
    }
}
