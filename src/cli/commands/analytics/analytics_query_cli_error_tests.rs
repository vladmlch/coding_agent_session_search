use crate::analytics;
use crate::cli::commands::analytics::fresh_franken_count_retry::analytics_query_cli_error;
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;

#[test]
pub(crate) fn analytics_query_cli_error_preserves_shape() {
    let err = analytics_query_cli_error(analytics::AnalyticsError::Db("boom".to_string()));

    assert_eq!(err.code, 9);
    assert_eq!(err.kind, CliErrorKind::DbError.kind_str());
    assert_eq!(err.message, "analytics db error: boom");
    assert_eq!(
        err.hint.as_deref(),
        Some("Check that the analytics tables exist and are not corrupt.")
    );
    assert!(!err.retryable);
}
