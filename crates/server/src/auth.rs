use crate::*;
use base64::Engine as _;

pub(crate) async fn health() -> impl IntoResponse {
    Json(json!({"status": "ok"}))
}

#[derive(Clone)]
pub(crate) struct ConsoleCredentials {
    username: String,
    password: String,
}

impl ConsoleCredentials {
    pub(crate) fn from_environment() -> Self {
        Self {
            username: env::var("HIGHWATER_CONSOLE_USERNAME").unwrap_or_else(|_| "demo".to_owned()),
            password: env::var("HIGHWATER_CONSOLE_PASSWORD").unwrap_or_else(|_| "demo".to_owned()),
        }
    }
}

pub(crate) async fn require_console_login(
    State(expected): State<Arc<ConsoleCredentials>>,
    request: Request,
    next: Next,
) -> Response {
    let supplied = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if console_credentials_match(supplied, &expected) {
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "invalid console username or password"})),
    )
        .into_response()
}

fn console_credentials_match(authorization: Option<&str>, expected: &ConsoleCredentials) -> bool {
    authorization
        .and_then(|value| value.strip_prefix("Basic "))
        .and_then(|value| base64::engine::general_purpose::STANDARD.decode(value).ok())
        .and_then(|value| String::from_utf8(value).ok())
        .and_then(|value| {
            value
                .split_once(':')
                .map(|(user, pass)| (user.to_owned(), pass.to_owned()))
        })
        .is_some_and(|(username, password)| {
            constant_time_eq(username.as_bytes(), expected.username.as_bytes())
                && constant_time_eq(password.as_bytes(), expected.password.as_bytes())
        })
}

pub(crate) async fn console_cors(request: Request, next: Next) -> Response {
    let preflight = request.method() == axum::http::Method::OPTIONS;
    let origin = request
        .headers()
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .filter(|value| allowed_console_origin(value))
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
            axum::http::HeaderValue::from_static("GET, OPTIONS"),
        );
        headers.insert(
            axum::http::header::ACCESS_CONTROL_ALLOW_HEADERS,
            axum::http::HeaderValue::from_static("Authorization"),
        );
        headers.insert(
            axum::http::header::VARY,
            axum::http::HeaderValue::from_static("Origin"),
        );
    }
    response
}

fn allowed_console_origin(origin: &str) -> bool {
    origin == "https://demo.highwater.cloud"
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
    fn demo_console_is_the_only_production_browser_origin() {
        assert!(allowed_console_origin("https://demo.highwater.cloud"));
        assert!(allowed_console_origin("http://localhost:8080"));
        assert!(!allowed_console_origin("https://highwater.cloud"));
        assert!(!allowed_console_origin(
            "https://demo.highwater.cloud.attacker.test"
        ));
    }

    #[test]
    fn console_login_requires_both_credentials() {
        let expected = ConsoleCredentials {
            username: "demo".to_owned(),
            password: "demo".to_owned(),
        };
        assert!(console_credentials_match(
            Some("Basic ZGVtbzpkZW1v"),
            &expected
        ));
        assert!(!console_credentials_match(
            Some("Basic ZGVtbzp3cm9uZw=="),
            &expected
        ));
        assert!(!console_credentials_match(
            Some("Bearer ZGVtbzpkZW1v"),
            &expected
        ));
        assert!(!console_credentials_match(None, &expected));
    }
}
