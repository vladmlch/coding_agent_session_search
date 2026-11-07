use crate::cli::commands::search::toon_encode_options_from_env::normalize_agent_filters;
use crate::doctor::lexical_cleanup_disposition_label::capabilities_connector_names;
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;

pub(crate) fn names(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

#[test]
pub(crate) fn agent_filter_aliases_resolve_to_archive_slugs() {
    let filters = names(&[
        " Claude-Code ",
        "claude",
        "Claude_Code",
        "CODEX",
        "pi-agent",
        "Oh-My-Pi",
        "ohmypi",
        "open-claw",
        "Open_Claw",
        "UNKNOWN",
    ]);
    assert_eq!(
        normalize_agent_filters(&filters).unwrap(),
        names(&[
            "claude_code",
            "codex",
            "pi_agent",
            "omp",
            "openclaw",
            "unknown"
        ])
    );
    assert!(normalize_agent_filters(&[]).unwrap().is_empty());
}

#[test]
pub(crate) fn agent_filter_every_advertised_connector_round_trips() {
    for slug in capabilities_connector_names() {
        let spellings = vec![slug.clone(), slug.to_ascii_uppercase().replace('_', "-")];
        assert_eq!(normalize_agent_filters(&spellings).unwrap(), vec![slug]);
    }
}

#[test]
pub(crate) fn agent_filter_unknown_values_fail_without_broadening_the_scope() {
    for filters in [
        names(&[""]),
        names(&["  "]),
        names(&["claud_code"]),
        names(&["claude_code", "claud_code"]),
        names(&["claud_code", "codex"]),
        names(&["未登録"]),
    ] {
        let error = normalize_agent_filters(&filters).unwrap_err();
        assert_eq!(error.code, 2);
        assert_eq!(error.kind, CliErrorKind::UnknownAgent.kind_str());
        assert!(!error.retryable);
        assert!(error.hint.as_deref().unwrap().contains("Known agents:"));
    }
    let error = normalize_agent_filters(&names(&["claud_code"])).unwrap_err();
    assert!(error.hint.as_deref().unwrap().contains("claude_code"));
}
