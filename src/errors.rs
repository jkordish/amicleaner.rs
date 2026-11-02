//! Error type and helpers that turn AWS SDK errors into readable messages.

use std::error::Error;
use std::fmt::Debug;

use aws_sdk_ec2 as ec2;
use ec2::error::{ProvideErrorMetadata, SdkError};

type DynError = Box<dyn Error + Send + Sync + 'static>;

pub(crate) type AppResult<T> = Result<T, DynError>;

/// Renders an error with its whole `source()` chain. `SdkError`'s own `Display` is only
/// "service error", so the useful detail lives further down.
pub(crate) fn error_chain(error: &(dyn Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !message.contains(&text) {
            message.push_str(": ");
            message.push_str(&text);
        }
        source = cause.source();
    }
    message
}

/// `Code: message` for service errors, otherwise the error chain (timeouts, dispatch failures).
pub(crate) fn describe_sdk_error<E, R>(error: &SdkError<E, R>) -> String
where
    E: ProvideErrorMetadata + Error + 'static,
    R: Debug + 'static,
{
    if let SdkError::ServiceError(_) = error {
        match (error.code(), error.message()) {
            (Some(code), Some(message)) => return format!("{code}: {message}"),
            (Some(code), None) => return code.to_string(),
            _ => {}
        }
    }
    error_chain(error)
}

pub(crate) trait AwsResultExt<T> {
    /// Converts an SDK error into an app error that names the operation and keeps the error code.
    fn aws_context(self, operation: &str) -> AppResult<T>;
}

impl<T, E, R> AwsResultExt<T> for Result<T, SdkError<E, R>>
where
    E: ProvideErrorMetadata + Error + 'static,
    R: Debug + 'static,
{
    fn aws_context(self, operation: &str) -> AppResult<T> {
        self.map_err(|error| format!("{operation} failed: {}", describe_sdk_error(&error)).into())
    }
}
