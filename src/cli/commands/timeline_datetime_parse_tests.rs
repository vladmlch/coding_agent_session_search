use crate::cli::commands::search::timefilter::{TimeBound, parse_time_bound};

#[test]
pub(crate) fn signed_relative_offsets_use_magnitude() {
    let positive = parse_time_bound("7d", TimeBound::Since).expect("7d parses");
    let negative = parse_time_bound("-7d", TimeBound::Since).expect("-7d parses");

    assert!(
        positive.abs_diff(negative) < 2_000,
        "7d and -7d should resolve to the same past window, got {positive} vs {negative}"
    );
}

/// `--since 5m` used to fall back to the 7-day default with exit 0.
#[test]
pub(crate) fn minutes_are_minutes_ago() {
    let now = chrono::Utc::now().timestamp_millis();
    for raw in ["5m", "-5m", "5 minutes ago"] {
        let start = parse_time_bound(raw, TimeBound::Since).expect("minutes parse");
        assert!(
            (now - start - 300_000).abs() < 60_000,
            "{raw} should be five minutes ago, got {} ms before now",
            now - start
        );
    }
    assert!(parse_time_bound("5x", TimeBound::Since).is_none());
}

#[test]
pub(crate) fn overflowing_relative_offsets_are_rejected_without_panicking() {
    assert!(parse_time_bound("-9223372036854775808d", TimeBound::Since).is_none());
    assert!(parse_time_bound("9223372036854775807h", TimeBound::Since).is_none());
}
