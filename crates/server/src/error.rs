use crate::*;
#[derive(Debug)]
pub(crate) struct ApiError(pub(crate) anyhow::Error);

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
        let status = if self.0.downcast_ref::<WatermarkAlignmentError>().is_some()
            || self.0.downcast_ref::<StreamCapacityError>().is_some()
        {
            StatusCode::TOO_MANY_REQUESTS
        } else {
            StatusCode::BAD_REQUEST
        };
        (status, Json(json!({"error": format!("{:#}", self.0)}))).into_response()
    }
}
