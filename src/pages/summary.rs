//! Pre-Publish Summary generation for pages export.
//!
//! Generates a comprehensive summary of all content that will be published,
//! enabling users to review and modify their selection before proceeding.
//!
//! # Overview
//!
//! The summary provides:
//! - **Quantitative metrics**: Total conversations, messages, and estimated size
//! - **Temporal scope**: Date range and activity histogram
//! - **Content categorization**: Breakdown by workspace and agent
//! - **Security status**: Encryption configuration and secret scan results
//!
//! # Example
//!
//! ```ignore
//! use crate::pages::summary::{PrePublishSummary, SummaryGenerator};
//!
//! let generator = SummaryGenerator::new(&db_conn);
//! let summary = generator.generate(None)?;
//! println!("{}", summary.render_overview());
//! ```

mod exclusions;
mod generator;
mod types;

#[cfg(test)]
mod tests;

pub use exclusions::ExclusionSet;
pub use generator::SummaryGenerator;
pub use types::{
    AgentSummaryItem, DateHistogramEntry, DateRange, EncryptionSummary, KeySlotSummary,
    KeySlotType, PrePublishSummary, ScanReportSummary, SummaryFilters, WorkspaceSummaryItem,
    estimate_compressed_size, format_size,
};
