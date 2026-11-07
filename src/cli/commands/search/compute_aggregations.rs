use crate::cli::args_sub::{AggregateField, AggregationBucket, Aggregations, FieldAggregation};
use crate::cli::error::{CliError, CliResult};

/// Compute aggregations from search hits
pub(crate) fn compute_aggregations(
    hits: &[crate::search::query::SearchHit],
    fields: &[AggregateField],
) -> Aggregations {
    use std::collections::HashMap;

    const MAX_BUCKETS: usize = 10;
    let mut aggregations = Aggregations::default();

    for field in fields {
        let mut counts: HashMap<String, u64> = HashMap::new();

        // Count occurrences based on field type
        for hit in hits {
            let key = match field {
                AggregateField::Agent => hit.agent.clone(),
                AggregateField::Workspace => hit.workspace.clone(),
                AggregateField::Date => {
                    // Group by date (YYYY-MM-DD)
                    hit.created_at
                        .and_then(|ts| {
                            chrono::DateTime::from_timestamp_millis(ts)
                                .map(|d| d.format("%Y-%m-%d").to_string())
                        })
                        .unwrap_or_else(|| "unknown".to_string())
                }
                AggregateField::MatchType => format!("{:?}", hit.match_type).to_lowercase(),
            };
            *counts.entry(key).or_insert(0) += 1;
        }

        // Sort by count descending, take top N
        let mut sorted: Vec<_> = counts.into_iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        let total_count: u64 = sorted.iter().map(|(_, c)| *c).sum();
        let top_buckets: Vec<AggregationBucket> = sorted
            .iter()
            .take(MAX_BUCKETS)
            .map(|(key, count)| AggregationBucket {
                key: key.clone(),
                count: *count,
            })
            .collect();
        let top_sum: u64 = top_buckets.iter().map(|b| b.count).sum();
        let other_count = total_count.saturating_sub(top_sum);

        let agg = FieldAggregation {
            buckets: top_buckets,
            other_count,
        };

        match field {
            AggregateField::Agent => aggregations.agent = Some(agg),
            AggregateField::Workspace => aggregations.workspace = Some(agg),
            AggregateField::Date => aggregations.date = Some(agg),
            AggregateField::MatchType => aggregations.match_type = Some(agg),
        }
    }

    aggregations
}

pub(crate) const VALID_AGGREGATE_FIELDS: &str = "agent, workspace, date, match_type";

/// Parse aggregate field strings into enum values.
pub(crate) fn parse_aggregate_fields(fields: &[String]) -> CliResult<Vec<AggregateField>> {
    let mut parsed = Vec::with_capacity(fields.len());
    let mut invalid = Vec::new();

    for field in fields {
        let trimmed = field.trim();
        if trimmed.is_empty() {
            invalid.push("<empty>".to_string());
            continue;
        }

        match AggregateField::from_str(trimmed) {
            Some(field) => parsed.push(field),
            None => invalid.push(field.clone()),
        }
    }

    if invalid.is_empty() {
        Ok(parsed)
    } else {
        Err(CliError::usage(
            format!("invalid --aggregate field(s): {}", invalid.join(", ")),
            Some(format!("Valid aggregate fields: {VALID_AGGREGATE_FIELDS}")),
        ))
    }
}
