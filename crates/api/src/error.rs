//! HTTP error mapping for domain errors.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use djq_core::{ErrorCategory, QueueError};
use serde::Serialize;

/// A transport-level error with a stable JSON shape.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    category: &'static str,
    message: String,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
    category: &'a str,
}

impl ApiError {
    pub fn new(status: StatusCode, category: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            category,
            message: message.into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }
}

impl From<QueueError> for ApiError {
    fn from(e: QueueError) -> Self {
        let (status, category) = match e.category() {
            ErrorCategory::BadRequest => (StatusCode::BAD_REQUEST, "bad_request"),
            ErrorCategory::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            ErrorCategory::Conflict => (StatusCode::CONFLICT, "conflict"),
            ErrorCategory::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %e, "internal error");
        }
        Self {
            status,
            category,
            message: e.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: &self.message,
            category: self.category,
        };
        (self.status, Json(body)).into_response()
    }
}

/// Convenience alias for handler results.
pub type ApiResult<T> = std::result::Result<T, ApiError>;
