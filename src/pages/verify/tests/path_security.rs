use super::super::integrity::check_integrity;
use super::super::paths::{
    compute_file_hash, contains_unicode_path_attack, detect_encoded_path_violation,
};
use super::assert_integrity_path_blocked;
use crate::pages::bundle::{IntegrityEntry, IntegrityManifest};
use std::collections::BTreeMap;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_integrity_path_traversal_blocked() {
    use std::collections::BTreeMap;

    let temp = TempDir::new().unwrap();
    let site_dir = temp.path();

    // Create integrity.json with path traversal attempt
    let mut files = BTreeMap::new();
    files.insert(
        "../../../etc/passwd".to_string(),
        crate::pages::bundle::IntegrityEntry {
            sha256: "deadbeef".repeat(8),
            size: 100,
        },
    );
    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2025-01-01T00:00:00Z".to_string(),
        files,
    };
    let manifest_json = serde_json::to_string(&manifest).unwrap();
    fs::write(site_dir.join("integrity.json"), manifest_json).unwrap();

    // Verify the check catches the path traversal
    let result = check_integrity(site_dir, false);
    assert!(!result.passed, "Path traversal should be blocked");
    assert!(
        result
            .details
            .as_ref()
            .map(|d| d.contains("security violation"))
            .unwrap_or(false),
        "Should mention security violation"
    );
}

#[test]
fn test_integrity_absolute_path_blocked() {
    use std::collections::BTreeMap;

    let temp = TempDir::new().unwrap();
    let site_dir = temp.path();

    // Create integrity.json with absolute path
    let mut files = BTreeMap::new();
    files.insert(
        "/etc/passwd".to_string(),
        crate::pages::bundle::IntegrityEntry {
            sha256: "deadbeef".repeat(8),
            size: 100,
        },
    );
    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2025-01-01T00:00:00Z".to_string(),
        files,
    };
    let manifest_json = serde_json::to_string(&manifest).unwrap();
    fs::write(site_dir.join("integrity.json"), manifest_json).unwrap();

    // Verify the check catches the absolute path
    let result = check_integrity(site_dir, false);
    assert!(!result.passed, "Absolute path should be blocked");
    assert!(
        result
            .details
            .as_ref()
            .map(|d| d.contains("security violation"))
            .unwrap_or(false),
        "Should mention security violation"
    );
}

#[test]
fn test_integrity_url_encoded_traversal_blocked_single() {
    assert_integrity_path_blocked("%2e%2e/%2e%2e/etc/passwd");
}

#[test]
fn test_integrity_url_encoded_traversal_blocked_double() {
    assert_integrity_path_blocked("%252e%252e/%252e%252e/etc/passwd");
}

#[test]
fn test_integrity_url_encoded_traversal_blocked_mixed() {
    assert_integrity_path_blocked("%2e./etc/passwd");
    assert_integrity_path_blocked(".%2e/etc/passwd");
    assert_integrity_path_blocked("..%2fetc/passwd");
}

#[test]
fn test_integrity_url_encoded_traversal_blocked_uppercase() {
    assert_integrity_path_blocked("%2E%2E/%2Fetc/passwd");
}

#[test]
fn test_integrity_url_encoded_traversal_blocked_overlong_utf8() {
    assert_integrity_path_blocked("%c0%ae%c0%ae/%c0%ae%c0%ae/etc/passwd");
}

#[test]
fn test_integrity_url_encoded_traversal_blocked_null_byte() {
    assert_integrity_path_blocked("valid%00/../etc/passwd");
}

#[test]
fn test_integrity_url_encoded_traversal_blocked_backslash() {
    assert_integrity_path_blocked("..\\..\\etc\\passwd");
    assert_integrity_path_blocked("..%5c..%5cetc%5cpasswd");
}

#[test]
fn test_integrity_url_encoded_traversal_blocked_separator_confusion() {
    assert_integrity_path_blocked(r"..\/..\/etc\/passwd");
}

// --- Unicode normalization attack tests ---

#[test]
fn test_integrity_unicode_fullwidth_dots_blocked() {
    // U+FF0E FULLWIDTH FULL STOP looks like '.' but is a different codepoint.
    // Two fullwidth dots form a visual ".." that bypasses naive ASCII checks.
    assert_integrity_path_blocked("\u{FF0E}\u{FF0E}/etc/passwd");
}

#[test]
fn test_integrity_unicode_fullwidth_slash_blocked() {
    // U+FF0F FULLWIDTH SOLIDUS looks like '/' but is a different codepoint.
    assert_integrity_path_blocked("payload\u{FF0F}..\\..\\etc\\passwd");
}

#[test]
fn test_integrity_unicode_fullwidth_backslash_blocked() {
    // U+FF3C FULLWIDTH REVERSE SOLIDUS looks like '\' but is a different codepoint.
    assert_integrity_path_blocked("payload\u{FF3C}..\\..\\etc\\passwd");
}

#[test]
fn test_integrity_unicode_small_full_stop_blocked() {
    // U+FE52 SMALL FULL STOP - a compatibility variant of '.'
    assert_integrity_path_blocked("\u{FE52}\u{FE52}/etc/passwd");
}

#[test]
fn test_integrity_unicode_one_dot_leader_blocked() {
    // U+2024 ONE DOT LEADER - looks nearly identical to '.'
    assert_integrity_path_blocked("\u{2024}\u{2024}/etc/passwd");
}

#[test]
fn test_integrity_unicode_halfwidth_ideographic_full_stop_blocked() {
    // U+FF61 HALFWIDTH IDEOGRAPHIC FULL STOP
    assert_integrity_path_blocked("\u{FF61}\u{FF61}/etc/passwd");
}

#[test]
fn test_integrity_unicode_mixed_fullwidth_and_ascii_blocked() {
    // Mix fullwidth and ASCII dots — the fullwidth char alone should trigger
    assert_integrity_path_blocked(".\u{FF0E}/etc/passwd");
    assert_integrity_path_blocked("\u{FF0E}./etc/passwd");
}

#[test]
fn test_integrity_percent_encoded_unicode_fullwidth_dot_blocked() {
    // Percent-encoded UTF-8 for U+FF0E (FULLWIDTH FULL STOP): 0xEF 0xBC 0x8E
    assert_integrity_path_blocked("%ef%bc%8e%ef%bc%8e/etc/passwd");
}

// --- Case sensitivity / Windows path tests ---

#[test]
fn test_integrity_windows_drive_letter_blocked() {
    assert_integrity_path_blocked("C:\\Windows\\System32\\config\\SAM");
}

#[test]
fn test_integrity_windows_drive_letter_lowercase_blocked() {
    assert_integrity_path_blocked("c:\\windows\\system32");
}

#[test]
fn test_integrity_windows_drive_letter_forward_slash_blocked() {
    assert_integrity_path_blocked("C:/Windows/System32");
}

#[test]
fn test_integrity_windows_unc_path_blocked() {
    // UNC paths start with \\ — should be caught as absolute
    assert_integrity_path_blocked("\\\\server\\share\\file.txt");
}

// --- Symlink traversal tests ---

#[test]
#[cfg(unix)]
fn test_integrity_symlink_traversal_blocked() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let site_dir = temp.path();

    // Create a target file outside the site directory
    let outside_dir = TempDir::new().unwrap();
    let secret_file = outside_dir.path().join("secret.txt");
    fs::write(&secret_file, "sensitive data").unwrap();

    // Create a symlink inside the site directory that points outside
    let link_path = site_dir.join("evil_link.txt");
    symlink(&secret_file, &link_path).unwrap();

    // Compute hash of the file the symlink points to
    let hash = compute_file_hash(&link_path).unwrap();
    let size = fs::metadata(&link_path).unwrap().len();

    let mut files = BTreeMap::new();
    files.insert(
        "evil_link.txt".to_string(),
        IntegrityEntry { sha256: hash, size },
    );
    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2025-01-01T00:00:00Z".to_string(),
        files,
    };
    fs::write(
        site_dir.join("integrity.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();

    // The canonicalize check should detect the symlink escapes site_dir
    let result = check_integrity(site_dir, false);
    assert!(
        !result.passed,
        "Symlink traversal outside site_dir should be blocked"
    );
    assert!(
        result
            .details
            .as_ref()
            .map(|d| d.contains("security violation"))
            .unwrap_or(false),
        "Should mention security violation for symlink escape"
    );
}

#[test]
#[cfg(unix)]
fn test_integrity_symlink_within_site_dir_allowed() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let site_dir = temp.path();

    // Create a real file inside site_dir
    let real_file = site_dir.join("real.txt");
    fs::write(&real_file, "legitimate data").unwrap();

    // Create a symlink that points to a file inside site_dir
    let link_path = site_dir.join("link_to_real.txt");
    symlink(&real_file, &link_path).unwrap();

    let hash = compute_file_hash(&link_path).unwrap();
    let size = fs::metadata(&link_path).unwrap().len();

    let mut files = BTreeMap::new();
    files.insert(
        "link_to_real.txt".to_string(),
        IntegrityEntry { sha256: hash, size },
    );
    // Also include the real file and integrity.json in manifest
    let real_hash = compute_file_hash(&real_file).unwrap();
    let real_size = fs::metadata(&real_file).unwrap().len();
    files.insert(
        "real.txt".to_string(),
        IntegrityEntry {
            sha256: real_hash,
            size: real_size,
        },
    );

    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2025-01-01T00:00:00Z".to_string(),
        files,
    };
    fs::write(
        site_dir.join("integrity.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();

    // Symlink within site_dir should be OK
    let result = check_integrity(site_dir, false);
    assert!(
        result.passed,
        "Symlink within site_dir should be allowed: {:?}",
        result.details
    );
}

// --- False positive tests: legitimate paths should NOT be blocked ---

#[test]
fn test_integrity_legitimate_dotted_version_not_blocked() {
    // "v2.1.0" contains dots but they're version numbers, not traversal
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path();
    let target = site_dir.join("assets/v2.1.0/bundle.js");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::write(&target, "// bundle").unwrap();

    let hash = compute_file_hash(&target).unwrap();
    let size = fs::metadata(&target).unwrap().len();
    let mut files = BTreeMap::new();
    files.insert(
        "assets/v2.1.0/bundle.js".to_string(),
        IntegrityEntry { sha256: hash, size },
    );

    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2025-01-01T00:00:00Z".to_string(),
        files,
    };
    fs::write(
        site_dir.join("integrity.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();

    let result = check_integrity(site_dir, false);
    assert!(
        result.passed,
        "Dotted version path should not be blocked: {:?}",
        result.details
    );
}

#[test]
fn test_integrity_legitimate_hidden_file_not_blocked() {
    // ".nojekyll" starts with a dot — should not be confused with traversal
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path();
    let target = site_dir.join(".nojekyll");
    fs::write(&target, "").unwrap();

    let hash = compute_file_hash(&target).unwrap();
    let size = fs::metadata(&target).unwrap().len();
    let mut files = BTreeMap::new();
    files.insert(
        ".nojekyll".to_string(),
        IntegrityEntry { sha256: hash, size },
    );

    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2025-01-01T00:00:00Z".to_string(),
        files,
    };
    fs::write(
        site_dir.join("integrity.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();

    let result = check_integrity(site_dir, false);
    assert!(
        result.passed,
        "Hidden file (.nojekyll) should not be blocked: {:?}",
        result.details
    );
}

#[test]
fn test_integrity_legitimate_payload_subdir_not_blocked() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path();
    let target = site_dir.join("payload/data/sessions.db");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::write(&target, "sqlite").unwrap();

    let hash = compute_file_hash(&target).unwrap();
    let size = fs::metadata(&target).unwrap().len();
    let mut files = BTreeMap::new();
    files.insert(
        "payload/data/sessions.db".to_string(),
        IntegrityEntry { sha256: hash, size },
    );

    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2025-01-01T00:00:00Z".to_string(),
        files,
    };
    fs::write(
        site_dir.join("integrity.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();

    let result = check_integrity(site_dir, false);
    assert!(
        result.passed,
        "Legitimate payload subdirectory should not be blocked: {:?}",
        result.details
    );
}

#[test]
fn test_integrity_legitimate_hyphens_underscores_not_blocked() {
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path();
    let target = site_dir.join("css/main-v2_final.css");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::write(&target, "body{}").unwrap();

    let hash = compute_file_hash(&target).unwrap();
    let size = fs::metadata(&target).unwrap().len();
    let mut files = BTreeMap::new();
    files.insert(
        "css/main-v2_final.css".to_string(),
        IntegrityEntry { sha256: hash, size },
    );

    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2025-01-01T00:00:00Z".to_string(),
        files,
    };
    fs::write(
        site_dir.join("integrity.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();

    let result = check_integrity(site_dir, false);
    assert!(
        result.passed,
        "Path with hyphens/underscores should not be blocked: {:?}",
        result.details
    );
}

// --- Unit tests for helper functions ---

#[test]
fn test_contains_unicode_path_attack_detects_fullwidth_period() {
    assert!(contains_unicode_path_attack("\u{FF0E}"));
    assert!(contains_unicode_path_attack("foo\u{FF0E}bar"));
}

#[test]
fn test_contains_unicode_path_attack_detects_fullwidth_solidus() {
    assert!(contains_unicode_path_attack("\u{FF0F}"));
}

#[test]
fn test_contains_unicode_path_attack_detects_fullwidth_reverse_solidus() {
    assert!(contains_unicode_path_attack("\u{FF3C}"));
}

#[test]
fn test_contains_unicode_path_attack_detects_small_full_stop() {
    assert!(contains_unicode_path_attack("\u{FE52}"));
}

#[test]
fn test_contains_unicode_path_attack_detects_one_dot_leader() {
    assert!(contains_unicode_path_attack("\u{2024}"));
}

#[test]
fn test_contains_unicode_path_attack_allows_ascii() {
    assert!(!contains_unicode_path_attack("payload/chunk-00000.bin"));
    assert!(!contains_unicode_path_attack("../etc/passwd")); // traversal, but ASCII
    assert!(!contains_unicode_path_attack(".nojekyll"));
}

#[test]
fn test_detect_encoded_path_violation_unicode_attack() {
    let result = detect_encoded_path_violation("\u{FF0E}\u{FF0E}/etc/passwd");
    assert_eq!(result, Some("unicode normalization attack".to_string()));
}

#[test]
fn test_detect_encoded_path_violation_percent_encoded_unicode() {
    // %EF%BC%8E = UTF-8 encoding of U+FF0E (FULLWIDTH FULL STOP)
    let result = detect_encoded_path_violation("%ef%bc%8e%ef%bc%8e/etc/passwd");
    assert_eq!(
        result,
        Some("url-encoded unicode normalization attack".to_string())
    );
}

// --- Additional Unicode normalization attack tests (coding_agent_session_search-13za) ---

#[test]
fn test_integrity_unicode_combining_long_solidus_overlay_blocked() {
    // U+0338 COMBINING LONG SOLIDUS OVERLAY - could visually disguise characters
    assert_integrity_path_blocked(".\u{0338}./etc/passwd");
}

#[test]
fn test_integrity_unicode_combining_short_solidus_overlay_blocked() {
    // U+0337 COMBINING SHORT SOLIDUS OVERLAY
    assert_integrity_path_blocked(".\u{0337}./etc/passwd");
}

#[test]
fn test_integrity_unicode_rtl_override_blocked() {
    // U+202E RIGHT-TO-LEFT OVERRIDE - can visually reverse path display
    // This could make "etc/passwd/../" appear as a safe path when it's actually traversal
    assert_integrity_path_blocked("etc/passwd/\u{202E}../");
}

#[test]
fn test_integrity_unicode_ltr_override_blocked() {
    // U+202D LEFT-TO-RIGHT OVERRIDE - directional override
    assert_integrity_path_blocked("\u{202D}../etc/passwd");
}

#[test]
fn test_integrity_unicode_rtl_embedding_blocked() {
    // U+202B RIGHT-TO-LEFT EMBEDDING
    assert_integrity_path_blocked("\u{202B}../etc/passwd");
}

#[test]
fn test_integrity_unicode_rtl_isolate_blocked() {
    // U+2067 RIGHT-TO-LEFT ISOLATE
    assert_integrity_path_blocked("\u{2067}../etc/passwd");
}

#[test]
fn test_integrity_unicode_zero_width_joiner_blocked() {
    // U+200D ZERO WIDTH JOINER - invisible character that could split tokens
    assert_integrity_path_blocked(".\u{200D}./etc/passwd");
}

#[test]
fn test_integrity_unicode_zero_width_non_joiner_blocked() {
    // U+200C ZERO WIDTH NON-JOINER
    assert_integrity_path_blocked(".\u{200C}./etc/passwd");
}

#[test]
fn test_integrity_unicode_zero_width_space_blocked() {
    // U+200B ZERO WIDTH SPACE - invisible character
    assert_integrity_path_blocked("..\u{200B}/etc/passwd");
}

#[test]
fn test_integrity_unicode_bom_blocked() {
    // U+FEFF BYTE ORDER MARK (ZERO WIDTH NO-BREAK SPACE)
    assert_integrity_path_blocked("\u{FEFF}../etc/passwd");
}

#[test]
fn test_integrity_unicode_fraction_slash_blocked() {
    // U+2044 FRACTION SLASH - visually similar to /
    assert_integrity_path_blocked("..\u{2044}etc\u{2044}passwd");
}

#[test]
fn test_integrity_unicode_division_slash_blocked() {
    // U+2215 DIVISION SLASH - visually similar to /
    assert_integrity_path_blocked("..\u{2215}etc\u{2215}passwd");
}

#[test]
fn test_integrity_unicode_big_solidus_blocked() {
    // U+29F8 BIG SOLIDUS - another slash look-alike
    assert_integrity_path_blocked("..\u{29F8}etc\u{29F8}passwd");
}

#[test]
fn test_integrity_unicode_vai_full_stop_blocked() {
    // U+A60E VAI FULL STOP - dot look-alike
    assert_integrity_path_blocked("\u{A60E}\u{A60E}/etc/passwd");
}

#[test]
fn test_integrity_unicode_syriac_full_stop_blocked() {
    // U+0701 SYRIAC SUPRALINEAR FULL STOP - dot look-alike
    assert_integrity_path_blocked("\u{0701}\u{0701}/etc/passwd");
}

// --- NFD/NFC normalization form tests ---

#[test]
fn test_integrity_unicode_nfd_decomposed_not_exploitable() {
    // NFD decomposition of certain characters could potentially be exploited
    // For example, some characters have canonical decompositions
    // This test verifies that legitimate paths with accented chars work
    let temp = TempDir::new().unwrap();
    let site_dir = temp.path();

    // Create a file with an accented filename (NFC form - precomposed)
    let target = site_dir.join("café.txt");
    fs::write(&target, "coffee").unwrap();

    let hash = compute_file_hash(&target).unwrap();
    let size = fs::metadata(&target).unwrap().len();
    let mut files = BTreeMap::new();
    files.insert(
        "café.txt".to_string(),
        IntegrityEntry { sha256: hash, size },
    );

    let manifest = IntegrityManifest {
        version: 1,
        generated_at: "2025-01-01T00:00:00Z".to_string(),
        files,
    };
    fs::write(
        site_dir.join("integrity.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();

    // Legitimate accented filenames should be allowed
    let result = check_integrity(site_dir, false);
    assert!(
        result.passed,
        "Legitimate accented filename should be allowed: {:?}",
        result.details
    );
}

// --- Unit tests for extended helper functions ---

#[test]
fn test_contains_unicode_path_attack_detects_combining_overlay() {
    assert!(contains_unicode_path_attack("\u{0338}")); // COMBINING LONG SOLIDUS OVERLAY
    assert!(contains_unicode_path_attack("\u{0337}")); // COMBINING SHORT SOLIDUS OVERLAY
}

#[test]
fn test_contains_unicode_path_attack_detects_zero_width() {
    assert!(contains_unicode_path_attack("\u{200D}")); // ZERO WIDTH JOINER
    assert!(contains_unicode_path_attack("\u{200C}")); // ZERO WIDTH NON-JOINER
    assert!(contains_unicode_path_attack("\u{200B}")); // ZERO WIDTH SPACE
    assert!(contains_unicode_path_attack("\u{FEFF}")); // BOM
}

#[test]
fn test_contains_unicode_path_attack_detects_rtl_overrides() {
    assert!(contains_unicode_path_attack("\u{202E}")); // RTL OVERRIDE
    assert!(contains_unicode_path_attack("\u{202D}")); // LTR OVERRIDE
    assert!(contains_unicode_path_attack("\u{202B}")); // RTL EMBEDDING
    assert!(contains_unicode_path_attack("\u{2067}")); // RTL ISOLATE
}

#[test]
fn test_contains_unicode_path_attack_detects_confusable_slashes() {
    assert!(contains_unicode_path_attack("\u{2044}")); // FRACTION SLASH
    assert!(contains_unicode_path_attack("\u{2215}")); // DIVISION SLASH
    assert!(contains_unicode_path_attack("\u{29F8}")); // BIG SOLIDUS
}

#[test]
fn test_contains_unicode_path_attack_detects_confusable_dots() {
    assert!(contains_unicode_path_attack("\u{A60E}")); // VAI FULL STOP
    assert!(contains_unicode_path_attack("\u{0701}")); // SYRIAC SUPRALINEAR FULL STOP
    assert!(contains_unicode_path_attack("\u{0702}")); // SYRIAC SUBLINEAR FULL STOP
}

#[test]
fn test_detect_encoded_path_violation_rtl_override() {
    let result = detect_encoded_path_violation("etc/passwd/\u{202E}../");
    assert_eq!(result, Some("unicode normalization attack".to_string()));
}

#[test]
fn test_detect_encoded_path_violation_zero_width_joiner() {
    let result = detect_encoded_path_violation(".\u{200D}./etc/passwd");
    assert_eq!(result, Some("unicode normalization attack".to_string()));
}

#[test]
fn test_detect_encoded_path_violation_fraction_slash() {
    let result = detect_encoded_path_violation("..\u{2044}etc\u{2044}passwd");
    assert_eq!(result, Some("unicode normalization attack".to_string()));
}
