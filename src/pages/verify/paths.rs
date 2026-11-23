use anyhow::Result;
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::Path;

#[derive(Debug)]
enum PercentDecodeError {
    InvalidEncoding,
    InvalidUtf8,
    NullByte,
}

impl fmt::Display for PercentDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEncoding => write!(f, "invalid percent-encoding"),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 after percent-decoding"),
            Self::NullByte => write!(f, "null byte in decoded path"),
        }
    }
}

struct DecodeOutcome {
    decoded: String,
    changed: bool,
}

fn percent_decode_once(input: &str) -> Result<DecodeOutcome, PercentDecodeError> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    let mut changed = false;

    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(PercentDecodeError::InvalidEncoding);
            }
            let hi = bytes[i + 1];
            let lo = bytes[i + 2];
            let hex = [hi, lo];
            let hex_str =
                std::str::from_utf8(&hex).map_err(|_| PercentDecodeError::InvalidEncoding)?;
            let val =
                u8::from_str_radix(hex_str, 16).map_err(|_| PercentDecodeError::InvalidEncoding)?;
            out.push(val);
            i += 3;
            changed = true;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }

    if out.contains(&0) {
        return Err(PercentDecodeError::NullByte);
    }

    let decoded = String::from_utf8(out).map_err(|_| PercentDecodeError::InvalidUtf8)?;
    Ok(DecodeOutcome { decoded, changed })
}

fn contains_path_traversal_like(input: &str) -> bool {
    input.split(['/', '\\']).any(|segment| segment == "..")
}

fn is_absolute_like(input: &str) -> bool {
    let normalized = input.replace('\\', "/");
    if normalized.starts_with('/') || normalized.starts_with("//") {
        return true;
    }
    let bytes = normalized.as_bytes();
    bytes.len() >= 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic()
}

/// Check for Unicode characters that are visual look-alikes for path-sensitive
/// ASCII characters (`.`, `/`, `\`). These could bypass text-based path checks
/// on filesystems that perform Unicode compatibility normalization (NFKC).
pub(super) fn contains_unicode_path_attack(input: &str) -> bool {
    for ch in input.chars() {
        match ch {
            // Fullwidth look-alikes (NFKC maps to ASCII equivalents)
            '\u{FF0E}' // FULLWIDTH FULL STOP → .
            | '\u{FF0F}' // FULLWIDTH SOLIDUS → /
            | '\u{FF3C}' // FULLWIDTH REVERSE SOLIDUS → \
            // Small form variants
            | '\u{FE52}' // SMALL FULL STOP → .
            // Dot leaders / ellipsis components
            | '\u{2024}' // ONE DOT LEADER → .
            // Halfwidth forms
            | '\u{FF61}' // HALFWIDTH IDEOGRAPHIC FULL STOP
            // Combining characters that could modify path-sensitive chars
            | '\u{0338}' // COMBINING LONG SOLIDUS OVERLAY (could visually disguise)
            | '\u{0337}' // COMBINING SHORT SOLIDUS OVERLAY
            // Zero-width characters (invisible, could split tokens)
            | '\u{200D}' // ZERO WIDTH JOINER
            | '\u{200C}' // ZERO WIDTH NON-JOINER
            | '\u{200B}' // ZERO WIDTH SPACE
            | '\u{FEFF}' // BYTE ORDER MARK / ZERO WIDTH NO-BREAK SPACE
            // Right-to-left override (can visually reverse path display)
            | '\u{202E}' // RIGHT-TO-LEFT OVERRIDE
            | '\u{202D}' // LEFT-TO-RIGHT OVERRIDE
            | '\u{202C}' // POP DIRECTIONAL FORMATTING
            | '\u{202A}' // LEFT-TO-RIGHT EMBEDDING
            | '\u{202B}' // RIGHT-TO-LEFT EMBEDDING
            | '\u{2066}' // LEFT-TO-RIGHT ISOLATE
            | '\u{2067}' // RIGHT-TO-LEFT ISOLATE
            | '\u{2068}' // FIRST STRONG ISOLATE
            | '\u{2069}' // POP DIRECTIONAL ISOLATE
            // Confusable slash characters
            | '\u{2044}' // FRACTION SLASH (visually similar to /)
            | '\u{2215}' // DIVISION SLASH (visually similar to /)
            | '\u{29F8}' // BIG SOLIDUS
            | '\u{1735}' // PHILIPPINE SINGLE PUNCTUATION (looks like /)
            // Confusable dot characters
            | '\u{2E2E}' // REVERSED QUESTION MARK (can look like period in some fonts)
            | '\u{0701}' // SYRIAC SUPRALINEAR FULL STOP
            | '\u{0702}' // SYRIAC SUBLINEAR FULL STOP
            | '\u{A60E}' // VAI FULL STOP
            | '\u{10A50}' // KHAROSHTHI PUNCTUATION DOT
            => return true,
            _ => {}
        }
    }
    false
}

pub(super) fn detect_encoded_path_violation(rel_path: &str) -> Option<String> {
    if contains_path_traversal_like(rel_path) {
        return Some("path traversal".to_string());
    }
    if is_absolute_like(rel_path) {
        return Some("absolute path".to_string());
    }
    if contains_unicode_path_attack(rel_path) {
        return Some("unicode normalization attack".to_string());
    }

    if !rel_path.contains('%') {
        return None;
    }

    let mut current = rel_path.to_string();
    for _ in 0..3 {
        let outcome = match percent_decode_once(&current) {
            Ok(o) => o,
            Err(e) => return Some(e.to_string()),
        };
        if !outcome.changed {
            break;
        }
        current = outcome.decoded;
        if contains_path_traversal_like(&current) {
            return Some("url-encoded path traversal".to_string());
        }
        if is_absolute_like(&current) {
            return Some("url-encoded absolute path".to_string());
        }
        if contains_unicode_path_attack(&current) {
            return Some("url-encoded unicode normalization attack".to_string());
        }
        if !current.contains('%') {
            break;
        }
    }

    None
}

/// Compute SHA256 hash of a file
pub(super) fn compute_file_hash(path: &Path) -> Result<String> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    // sha2 ≥ 0.11 dropped `LowerHex` on the digest output;
    // `hex::encode` produces the same lowercase-hex representation.
    Ok(hex::encode(hasher.finalize()))
}

/// Collect all files in a directory recursively
pub(super) fn collect_all_files(dir: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();
    collect_files_recursive(dir, dir, &mut files)?;
    Ok(files)
}

fn collect_files_recursive(base: &Path, current: &Path, files: &mut Vec<String>) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        let file_type = metadata.file_type();

        if file_type.is_symlink() {
            if let Ok(rel) = path.strip_prefix(base) {
                files.push(rel.to_string_lossy().replace('\\', "/"));
            }
            continue;
        }

        if file_type.is_dir() {
            collect_files_recursive(base, &path, files)?;
        } else if file_type.is_file()
            && let Ok(rel) = path.strip_prefix(base)
        {
            files.push(rel.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
}

/// Calculate total size of a directory
pub(super) fn calculate_dir_size(dir: &Path) -> Result<u64> {
    let mut total = 0u64;

    fn calc_recursive(path: &Path, total: &mut u64) -> Result<()> {
        let metadata = fs::symlink_metadata(path)?;
        let file_type = metadata.file_type();

        if file_type.is_symlink() {
            return Ok(());
        }

        if file_type.is_dir() {
            for entry in fs::read_dir(path)? {
                calc_recursive(&entry?.path(), total)?;
            }
        } else if file_type.is_file() {
            *total += metadata.len();
        }
        Ok(())
    }

    calc_recursive(dir, &mut total)?;
    Ok(total)
}
