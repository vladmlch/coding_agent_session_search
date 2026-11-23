//! Verify command for CI pipelines.
//!
//! Provides `cass pages --verify <PATH>` to validate an existing export bundle for CI/CD.
//! The verifier confirms correct structure, config schema, payload integrity, and
//! the absence of secrets in site/.

mod checks;
mod integrity;
mod paths;
mod share_profile;
mod types;

#[cfg(test)]
mod tests;

pub use types::{
    CheckResult, ShareProfileEvidence, VerifyChecks, VerifyResult, ensure_valid_bundle,
    print_result, verify_bundle, with_verified_bundle_for_deployment,
};
