use crate::*;
#[derive(Debug)]
pub(crate) struct ApiError(pub(crate) anyhow::Error);

// A group commit may report one failure to several callers. Keep its source
// chain instead of converting it to text, so transport failures stay retryable.
#[derive(Debug, Clone)]
pub(crate) struct SharedTaskError(pub(crate) Arc<anyhow::Error>);

impl std::fmt::Display for SharedTaskError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:#}", self.0)
    }
}

impl std::error::Error for SharedTaskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}

#[derive(Debug)]
pub(crate) struct WatermarkAlignmentError(pub(crate) String);

#[derive(Debug)]
pub(crate) struct StreamCapacityError(pub(crate) String);

impl std::fmt::Display for WatermarkAlignmentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for WatermarkAlignmentError {}

impl std::fmt::Display for StreamCapacityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for StreamCapacityError {}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(value: E) -> Self {
        Self(value.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let message = format!("{:#}", self.0);
        let status = if self
            .0
            .chain()
            .any(|error| error.is::<object_store::Error>())
            || message.contains("conditional append was fenced")
            || message.starts_with("process partition ") && message.contains(" is fenced at epoch ")
        {
            StatusCode::SERVICE_UNAVAILABLE
        } else if self.0.downcast_ref::<WatermarkAlignmentError>().is_some()
            || self.0.downcast_ref::<StreamCapacityError>().is_some()
        {
            StatusCode::TOO_MANY_REQUESTS
        } else {
            StatusCode::BAD_REQUEST
        };
        (status, Json(json!({"error": message}))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_error_remains_retryable_through_shared_task_response() {
        let storage = object_store::Error::Generic {
            store: "test",
            source: std::io::Error::new(std::io::ErrorKind::ConnectionReset, "partitioned").into(),
        };
        let shared = SharedTaskError(Arc::new(
            anyhow::Error::new(storage).context("journal read failed"),
        ));
        let (sender, receiver) = std::sync::mpsc::channel::<Result<(), SharedTaskError>>();
        sender.send(Err(shared.clone())).unwrap();
        let error = receiver
            .recv()
            .unwrap()
            .map_err(anyhow::Error::new)
            .unwrap_err();
        assert_eq!(
            ApiError(error).into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            ApiError(anyhow::Error::new(shared))
                .into_response()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn shared_validation_error_stays_non_retryable() {
        let error = SharedTaskError(Arc::new(anyhow!("invalid process transition")));
        assert_eq!(
            ApiError(anyhow::Error::new(error)).into_response().status(),
            StatusCode::BAD_REQUEST
        );
    }
}
