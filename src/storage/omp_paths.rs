//! Pure Pi-family / OMP path ownership and profile-provenance helpers.
//!
//! Extracted from the legacy `connectors::omp` adapter so legacy archive
//! reclassification in `storage::sqlite` needs no live FAD connector.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Normalize an OMP profile name using the same contract as OMP v18.
#[must_use]
fn normalize_profile_name(profile: &str) -> Option<String> {
    let name = profile.trim();
    if name.is_empty() || name == "default" || name == "." || name == ".." {
        return None;
    }
    if name.ends_with('.') || name.len() > 64 {
        return None;
    }
    let mut chars = name.chars();
    if !chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        || !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c))
    {
        return None;
    }
    let base = name.split('.').next().unwrap_or(name);
    let upper = base.to_ascii_uppercase();
    let reserved = matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (upper.len() == 4
            && (upper.starts_with("COM") || upper.starts_with("LPT"))
            && upper.as_bytes()[3].is_ascii_digit());
    (!reserved).then(|| name.to_string())
}

/// Resolve the active profile without allowing an empty `OMP_PROFILE` to fall
/// through to legacy `PI_PROFILE`.
#[must_use]
fn active_profile_from_env() -> Option<String> {
    match dotenvy::var("OMP_PROFILE") {
        Ok(value) => normalize_profile_name(&value),
        Err(_) => dotenvy::var("PI_PROFILE")
            .ok()
            .and_then(|value| normalize_profile_name(&value)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PiFamilyOwner {
    Omp,
    PiAgent,
    Unknown,
}

/// Snapshot of the process-level Pi-family ownership inputs.
///
/// Keeping the snapshot separate from path classification makes precedence
/// explicit and ensures a scan does not observe a mixture of environment
/// values if another test or embedding process changes its environment.
#[derive(Debug, Clone)]
pub(crate) struct PiFamilyOwnership {
    pi_sessions_dir: Option<PathBuf>,
    omp_session_dir: Option<PathBuf>,
    shared_agent_dir: Option<PathBuf>,
    omp_config_root: Option<PathBuf>,
    omp_store_roots: Vec<(PathBuf, Option<String>)>,
}

impl PiFamilyOwnership {
    #[must_use]
    pub(crate) fn live() -> Self {
        let home = dirs::home_dir();
        let active_profile = active_profile_from_env();
        let config_name = dotenvy::var("PI_CONFIG_DIR")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| ".omp".to_string());
        let omp_config_root = home
            .as_ref()
            .map(|home| home.join(config_name.trim_start_matches(['/', '\\'])));
        let tagged_roots = local_omp_store_roots_from(
            home.as_deref(),
            nonempty_env_path("XDG_DATA_HOME").as_deref(),
            nonempty_env_path("PI_CODING_AGENT_SESSION_DIR").as_deref(),
            nonempty_env_path("CASS_OMP_DATA_ROOT").as_deref(),
            &config_name,
            active_profile.clone(),
        );

        Self {
            pi_sessions_dir: nonempty_env_path("PI_SESSIONS_DIR"),
            omp_session_dir: nonempty_env_path("PI_CODING_AGENT_SESSION_DIR"),
            shared_agent_dir: nonempty_env_path("PI_CODING_AGENT_DIR"),
            omp_config_root,
            omp_store_roots: tagged_roots,
        }
    }

    /// Resolve one session path to a single provider identity.
    ///
    /// Provider-specific overrides are checked before any layout heuristic.
    /// `PI_CODING_AGENT_DIR` is intentionally lower priority because both
    /// programs honor it; absent an OMP-specific signal it retains the legacy
    /// Pi Agent identity instead of being indexed once by each connector.
    #[must_use]
    pub(crate) fn owner(&self, path: &Path) -> PiFamilyOwner {
        if self
            .pi_sessions_dir
            .as_deref()
            .is_some_and(|root| path_is_within(path, root))
        {
            return PiFamilyOwner::PiAgent;
        }
        if self
            .omp_session_dir
            .as_deref()
            .is_some_and(|root| path_is_within(path, root))
        {
            return PiFamilyOwner::Omp;
        }
        if self
            .omp_store_roots
            .iter()
            .any(|(root, _)| path_is_within(path, root))
        {
            return PiFamilyOwner::Omp;
        }
        let archive_layout = classify_omp_archive_path(path);
        if self
            .omp_config_root
            .as_deref()
            .is_some_and(|root| path_is_within(path, root))
            || archive_layout == Some(OmpArchivePathClass::ConfigOrMirror)
        {
            return PiFamilyOwner::Omp;
        }
        if has_pi_agent_layout_marker(path) {
            return PiFamilyOwner::PiAgent;
        }
        if self.shared_agent_dir.as_deref().is_some_and(|root| {
            path_is_within(path, root) || path_is_within(path, &root.join("sessions"))
        }) {
            return PiFamilyOwner::PiAgent;
        }
        if archive_layout == Some(OmpArchivePathClass::Xdg) {
            return PiFamilyOwner::Omp;
        }
        PiFamilyOwner::Unknown
    }

    /// Stable, non-reversible identity of the live inputs that can change
    /// archive ownership. Storage binds its completed legacy-migration marker
    /// to this value so adding a custom XDG/CASS root later cannot strand an
    /// old Pi-labelled OMP row behind a stale fast path.
    #[must_use]
    pub(crate) fn archive_reclassification_context(&self) -> String {
        fn add_path(hasher: &mut blake3::Hasher, label: &str, path: Option<&Path>) {
            hasher.update(label.as_bytes());
            hasher.update(b"\0");
            if let Some(path) = path {
                hasher.update(path.to_string_lossy().as_bytes());
            }
            hasher.update(b"\0");
        }

        let mut hasher = blake3::Hasher::new();
        hasher.update(b"cass-pi-family-ownership-v1\0");
        add_path(
            &mut hasher,
            "pi_sessions_dir",
            self.pi_sessions_dir.as_deref(),
        );
        add_path(
            &mut hasher,
            "omp_session_dir",
            self.omp_session_dir.as_deref(),
        );
        add_path(
            &mut hasher,
            "shared_agent_dir",
            self.shared_agent_dir.as_deref(),
        );
        add_path(
            &mut hasher,
            "omp_config_root",
            self.omp_config_root.as_deref(),
        );
        for (root, _) in &self.omp_store_roots {
            add_path(&mut hasher, "omp_store_root", Some(root));
        }
        hasher.finalize().to_hex().to_string()
    }
}

fn nonempty_env_path(key: &str) -> Option<PathBuf> {
    dotenvy::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn path_is_within(path: &Path, root: &Path) -> bool {
    if let (Ok(canonical_path), Ok(canonical_root)) =
        (fs::canonicalize(path), fs::canonicalize(root))
    {
        return canonical_path.starts_with(canonical_root);
    }

    // Lexical fallback is needed for historical paths that no longer exist,
    // but it must not bless a prefix that a parent traversal subsequently
    // escapes. The structural classifier applies the same fail-closed rule.
    if path_parts(path).is_none() || path_parts(root).is_none() {
        return false;
    }
    path.starts_with(root)
}

fn path_parts(path: &Path) -> Option<Vec<String>> {
    // Archived paths may have been written on another operating system. Split
    // both separator styles explicitly instead of letting the current host's
    // `Path::components` reinterpret a Windows path as one Unix component.
    // A parent component invalidates structural evidence entirely: archived or
    // missing paths cannot be canonicalized reliably, and accepting a marker
    // before `..` would let the resolved path escape into another provider.
    let mut parts = Vec::new();
    for component in path
        .as_os_str()
        .to_string_lossy()
        .split(['/', '\\'])
        .filter(|component| !component.is_empty() && *component != ".")
    {
        if component == ".." {
            return None;
        }
        parts.push(component.to_owned());
    }
    Some(parts)
}

fn has_config_omp_layout_marker(parts: &[String]) -> bool {
    parts
        .windows(2)
        .any(|window| window[0] == ".omp" && window[1] == "agent")
        || parts.windows(4).any(|window| {
            window[0] == ".omp"
                && window[1] == "profiles"
                && normalize_profile_name(&window[2]).is_some()
                && window[3] == "agent"
        })
}

fn has_pi_agent_layout_marker(path: &Path) -> bool {
    path_parts(path).is_some_and(|parts| {
        parts
            .windows(2)
            .any(|parts| parts[0] == ".pi" && parts[1] == "agent")
    })
}

fn is_safe_mirror_hash(value: &str) -> bool {
    (8..=16).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn encoded_path_has_marker(encoded: &str, marker: &str) -> bool {
    encoded.match_indices(marker).any(|(index, _)| {
        let end = index + marker.len();
        (index == 0 || encoded.as_bytes()[index - 1] == b'_')
            && (end == encoded.len() || encoded.as_bytes()[end] == b'_')
    })
}

fn safe_mirror_name_has_omp_layout(name: &str) -> bool {
    let Some((encoded, hash)) = name.rsplit_once('_') else {
        return false;
    };
    if !is_safe_mirror_hash(hash) {
        return false;
    }

    encoded == ".omp"
        || encoded_path_has_marker(encoded, ".omp_agent")
        || encoded_path_has_marker(encoded, ".omp_profiles")
        || encoded_path_has_marker(encoded, ".local_share_omp")
}

fn safe_mirror_name_is_omp_profiles_root(name: &str) -> bool {
    let Some((encoded, hash)) = name.rsplit_once('_') else {
        return false;
    };
    is_safe_mirror_hash(hash)
        && encoded.ends_with(".omp_profiles")
        && encoded_path_has_marker(encoded, ".omp_profiles")
}

fn has_sanitized_omp_mirror_marker(parts: &[String]) -> bool {
    // Archived remote mirrors preserve the configured remote path in the
    // mirror container name. A tilde path starts with the provider marker,
    // while an absolute path retains its leading components, for example:
    //
    //   ~/.omp/agent/sessions       -> .omp_agent_sessions_<hash>
    //   /home/u/.omp/agent/sessions -> home_u_.omp_agent_sessions_<hash>
    //
    // Only trust the embedded absolute-path marker in the actual
    // `remotes/<source>/mirror/<safe-name>` slot. Treating the same substring
    // as an OMP signal in an arbitrary local directory would steal Pi-family
    // logs merely because an unrelated ancestor happened to contain `.omp`.
    parts.windows(4).any(|window| {
        window[0] == "remotes"
            && window[2] == "mirror"
            && safe_mirror_name_has_omp_layout(&window[3])
    })
}

fn has_xdg_omp_layout_marker(parts: &[String]) -> bool {
    // `XDG_DATA_HOME` is user-configurable, so the default `.local/share`
    // prefix cannot be the anchor: a relocated data home holds the same
    // `omp/{sessions,profiles}` app layout under an arbitrary parent, and
    // pinning the default prefix made those stores undetectable (the resume
    // path was the first victim — bead roq9y). The durable anchor is the
    // store-INTERNAL chain: `omp/sessions/<sanitized cwd>` (safe-dirname
    // encoding always yields a `-`-prefixed component for the absolute cwd)
    // or `omp/profiles/<valid profile name>/sessions`. Requiring the complete
    // store-internal chain keeps the anti-theft rule intact — a stray `omp`
    // ancestor with arbitrary children still classifies as nothing.
    parts.windows(4).any(|window| {
        window[0] == ".local"
            && window[1] == "share"
            && window[2] == "omp"
            && window[3] == "sessions"
    }) || parts.windows(6).any(|window| {
        window[0] == ".local"
            && window[1] == "share"
            && window[2] == "omp"
            && window[3] == "profiles"
            && normalize_profile_name(&window[4]).is_some()
            && window[5] == "sessions"
    }) || parts
        .windows(3)
        .any(|window| window[0] == "omp" && window[1] == "sessions" && window[2].starts_with('-'))
        || parts.windows(4).any(|window| {
            window[0] == "omp"
                && window[1] == "profiles"
                && normalize_profile_name(&window[2]).is_some()
                && window[3] == "sessions"
        })
}

/// Durable OMP evidence encoded in an archived transcript path.
///
/// This classifier is deliberately independent of the live process
/// environment so the connector ownership boundary and legacy SQLite
/// reclassification use exactly the same structural evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OmpArchivePathClass {
    /// A canonical `.omp` config layout or a production remote-mirror slot.
    ConfigOrMirror,
    /// The OMP XDG app layout, including a relocated `XDG_DATA_HOME`.
    Xdg,
}

#[must_use]
fn classify_omp_archive_path(path: &Path) -> Option<OmpArchivePathClass> {
    let parts = path_parts(path)?;
    if has_config_omp_layout_marker(&parts) || has_sanitized_omp_mirror_marker(&parts) {
        return Some(OmpArchivePathClass::ConfigOrMirror);
    }
    has_xdg_omp_layout_marker(&parts).then_some(OmpArchivePathClass::Xdg)
}

fn push_tagged_root(
    roots: &mut Vec<(PathBuf, Option<String>)>,
    path: PathBuf,
    profile: Option<String>,
) {
    if path.exists() {
        roots.push((path, profile));
    }
}

fn profile_directories(base: &Path) -> Vec<(String, PathBuf)> {
    let Ok(entries) = fs::read_dir(base) else {
        return Vec::new();
    };
    let mut profiles = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let file_type = entry.file_type().ok()?;
            if !file_type.is_dir() {
                return None;
            }
            let name = entry.file_name();
            let profile = normalize_profile_name(name.to_string_lossy().as_ref())?;
            Some((profile, entry.path()))
        })
        .collect::<Vec<_>>();
    profiles.sort_by(|left, right| left.0.cmp(&right.0));
    profiles
}

fn append_config_layout_roots(roots: &mut Vec<(PathBuf, Option<String>)>, config_root: &Path) {
    for (profile, profile_root) in profile_directories(&config_root.join("profiles")) {
        let agent_root = profile_root.join("agent");
        if agent_root.exists() {
            push_tagged_root(roots, agent_root, Some(profile));
        } else if profile_root.join("sessions").exists() {
            push_tagged_root(roots, profile_root, Some(profile));
        }
    }
    push_tagged_root(roots, config_root.join("agent"), None);
}

fn append_xdg_layout_roots(roots: &mut Vec<(PathBuf, Option<String>)>, app_root: &Path) {
    for (profile, profile_root) in profile_directories(&app_root.join("profiles")) {
        push_tagged_root(roots, profile_root, Some(profile));
    }
    push_tagged_root(roots, app_root.to_path_buf(), None);
}

/// Expand an explicitly OMP-qualified root without sweeping sibling Pi data.
fn declared_omp_store_roots(root: &Path) -> Vec<(PathBuf, Option<String>)> {
    let mut roots = Vec::new();

    append_config_layout_roots(&mut roots, &root.join(".omp"));
    append_xdg_layout_roots(&mut roots, &root.join(".local/share/omp"));
    append_xdg_layout_roots(&mut roots, &root.join("omp"));
    append_config_layout_roots(&mut roots, root);

    if root.join("sessions").exists()
        || root.file_name().is_some_and(|name| name == "sessions")
        || root.file_name().is_some_and(|name| name == "omp")
    {
        push_tagged_root(
            &mut roots,
            root.to_path_buf(),
            profile_from_session_path(root),
        );
    }

    // A provider-specific override may directly name a flat or not-yet-filled
    // store. Only fall back to the declared root when no narrower OMP layout
    // was found, so a copied home containing both `.pi` and `.omp` is not
    // recursively swept as OMP.
    if roots.is_empty() {
        push_tagged_root(
            &mut roots,
            root.to_path_buf(),
            profile_from_session_path(root),
        );
    }

    dedupe_tagged_roots(&mut roots);
    roots
}

fn local_omp_store_roots_from(
    home: Option<&Path>,
    xdg_data_home: Option<&Path>,
    omp_session_dir: Option<&Path>,
    cass_omp_data_root: Option<&Path>,
    config_name: &str,
    active_profile: Option<String>,
) -> Vec<(PathBuf, Option<String>)> {
    let mut roots = Vec::new();

    if let Some(session_dir) = omp_session_dir {
        push_tagged_root(&mut roots, session_dir.to_path_buf(), active_profile);
    }
    if let Some(root) = cass_omp_data_root {
        roots.extend(declared_omp_store_roots(root));
    }
    if let Some(home) = home {
        let config_root = home.join(config_name.trim_start_matches(['/', '\\']));
        append_config_layout_roots(&mut roots, &config_root);

        let xdg_root = xdg_data_home.map_or_else(
            || home.join(".local/share/omp"),
            |data_home| data_home.join("omp"),
        );
        append_xdg_layout_roots(&mut roots, &xdg_root);
    } else if let Some(data_home) = xdg_data_home {
        append_xdg_layout_roots(&mut roots, &data_home.join("omp"));
    }

    dedupe_tagged_roots(&mut roots);
    roots
}

fn dedupe_tagged_roots(roots: &mut Vec<(PathBuf, Option<String>)>) {
    let mut seen = HashSet::new();
    roots.retain(|(path, _)| {
        let key = fs::canonicalize(path).unwrap_or_else(|_| path.clone());
        seen.insert(key)
    });
}

/// Recover a valid named profile from canonical, custom-config, or XDG OMP
/// session paths. Callers must first establish that the path belongs to OMP.
#[must_use]
pub(crate) fn profile_from_session_path(path: &Path) -> Option<String> {
    let parts = path_parts(path)?;

    // Archived remote mirrors flatten the `~/.omp/profiles` preset into one
    // provider-qualified mirror container. The profile then sits directly
    // below the safe name instead of below a literal `profiles` component:
    //
    //   remotes/<source>/mirror/.omp_profiles_<hash>/<profile>/agent/sessions
    //
    // Recover it only from that exact production slot. A marker-shaped
    // directory elsewhere is not authoritative profile provenance.
    for window in parts.windows(7) {
        let [
            remotes,
            _source,
            mirror,
            safe_root,
            profile,
            agent,
            sessions,
        ] = window
        else {
            continue;
        };
        if remotes == "remotes"
            && mirror == "mirror"
            && safe_mirror_name_is_omp_profiles_root(safe_root)
            && agent == "agent"
            && sessions == "sessions"
        {
            return normalize_profile_name(profile);
        }
    }

    for window in parts.windows(4) {
        if window[0] == "profiles" && window[2] == "agent" && window[3] == "sessions" {
            return normalize_profile_name(&window[1]);
        }
    }
    for window in parts.windows(3) {
        if window[0] == "profiles" && window[2] == "sessions" {
            return normalize_profile_name(&window[1]);
        }
    }
    None
}
