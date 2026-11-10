use crate::doctor::policy::types::DoctorAssetClass;
use crate::doctor::raw_mirror::doctorrawmirrorverificationmode::doctor_raw_mirror_root;
use crate::doctor::repair_state::types::DoctorConfigExclusionTarget;
use std::path::Path;

pub(crate) fn doctor_config_exclusion_targets(
    data_dir: &Path,
    db_path: &Path,
) -> Vec<DoctorConfigExclusionTarget> {
    vec![
        DoctorConfigExclusionTarget {
            path: data_dir.to_path_buf(),
            asset_class: DoctorAssetClass::CanonicalArchiveDb,
            label: "cass data directory",
            tokens: &["cass", "cass-data", "data_dir", "data-dir"],
        },
        DoctorConfigExclusionTarget {
            path: db_path.to_path_buf(),
            asset_class: DoctorAssetClass::CanonicalArchiveDb,
            label: "canonical archive database",
            tokens: &["agent_search.db", "archive", "database"],
        },
        DoctorConfigExclusionTarget {
            path: doctor_raw_mirror_root(data_dir),
            asset_class: DoctorAssetClass::RawMirrorBlob,
            label: "raw mirror evidence",
            tokens: &["raw-mirror", "raw_mirror", "raw mirror"],
        },
        DoctorConfigExclusionTarget {
            path: data_dir.join("backups"),
            asset_class: DoctorAssetClass::BackupBundle,
            label: "doctor backups",
            tokens: &["backup", "backups"],
        },
        DoctorConfigExclusionTarget {
            path: data_dir.join("doctor").join("receipts"),
            asset_class: DoctorAssetClass::OperationReceipt,
            label: "doctor receipts",
            tokens: &["receipt", "receipts"],
        },
        DoctorConfigExclusionTarget {
            path: data_dir.join("doctor").join("forensic-bundles"),
            asset_class: DoctorAssetClass::ForensicBundle,
            label: "forensic bundles",
            tokens: &["forensic-bundle", "forensic_bundles", "forensic"],
        },
        DoctorConfigExclusionTarget {
            path: data_dir.join("doctor").join("support-bundles"),
            asset_class: DoctorAssetClass::SupportBundle,
            label: "support bundles",
            tokens: &["support-bundle", "support_bundles", "support"],
        },
    ]
}

/// `path` with `/` separators, `.` and `..` dropped. A Windows prefix is kept
/// (`C:`, `//server/share`; the verbatim forms render like the plain ones):
/// this string is the data dir in the archive-normalize fingerprint and in
/// the apply command doctor prints, and without the drive `C:\x` and `D:\x`
/// were one identity and `--data-dir /x` named the current drive (dddwv).
pub(crate) fn doctor_path_to_slash_string(path: &Path) -> String {
    use std::path::{Component, Prefix};

    let mut rendered = String::new();
    let mut needs_separator = false;
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => {
                match prefix.kind() {
                    Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => {
                        rendered.push(char::from(drive.to_ascii_uppercase()));
                        rendered.push(':');
                    }
                    Prefix::UNC(server, share) | Prefix::VerbatimUNC(server, share) => {
                        rendered.push_str("//");
                        rendered.push_str(&server.to_string_lossy());
                        rendered.push('/');
                        rendered.push_str(&share.to_string_lossy());
                    }
                    Prefix::Verbatim(name) => {
                        rendered.push_str("//?/");
                        rendered.push_str(&name.to_string_lossy());
                    }
                    Prefix::DeviceNS(name) => {
                        rendered.push_str("//./");
                        rendered.push_str(&name.to_string_lossy());
                    }
                }
                needs_separator = false;
            }
            Component::RootDir => {
                rendered.push('/');
                needs_separator = false;
            }
            Component::Normal(part) => {
                if needs_separator {
                    rendered.push('/');
                }
                rendered.push_str(&part.to_string_lossy());
                needs_separator = true;
            }
            Component::CurDir | Component::ParentDir => {}
        }
    }
    rendered
}
