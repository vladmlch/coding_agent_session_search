use anyhow::Result;
use serde_json::{Value, json};

use super::super::protocol::refuse;
use super::CURSOR_MAX_BYTES;

// ---------------------------------------------------------------------------
// Cursor (generation-bound keyset)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(super) struct SearchCursor {
    pub(super) value: Option<f64>,
    pub(super) null_flag: bool,
    pub(super) id: i64,
}

pub(super) fn encode_cursor(
    sort: &str,
    cursor: &SearchCursor,
    generation: &super::super::protocol::Generation,
    digest: &str,
) -> String {
    json!({
        // ss2: value travels as its f64 bit pattern (see the comment below);
        // ss1 cursors (decimal float "v") are refused as cursor_expired.
        "k": "ss2",
        "s": sort,
        "nf": cursor.null_flag,
        // The sort value travels as its exact f64 bit pattern, never as a
        // decimal float: serde_json's float PARSE is not correctly rounded
        // for every f64 (the significand x pow10 path can land one ULP off),
        // and a keyset bound even one ULP below the true key makes the walk
        // strand every entry that ties the bound score — 302 members of
        // `cargo` on the owner archive were never emitted by a full
        // cursor walk (a one-ULP-HIGH bound instead re-emits ties forever,
        // the cycling the incumbent binary showed). Integer parse of the
        // bit pattern is exact, so encode -> decode is bit-identical.
        "v": cursor.value.map(f64::to_bits),
        "id": cursor.id,
        "g": generation,
        "q": digest,
    })
    .to_string()
}

pub(super) fn decode_cursor(
    raw: &str,
    sort: &str,
    generation: &super::super::protocol::Generation,
    digest: &str,
) -> Result<SearchCursor> {
    let expired = || refuse("cursor_expired", "Invalid or stale search cursor");
    if raw.len() > CURSOR_MAX_BYTES || !raw.is_ascii() {
        return Err(expired());
    }
    let value: Value = serde_json::from_str(raw).map_err(|_| expired())?;
    let object = value.as_object().ok_or_else(expired)?;
    if object.get("k").and_then(Value::as_str) != Some("ss2")
        || object.get("s").and_then(Value::as_str) != Some(sort)
        || object.get("q").and_then(Value::as_str) != Some(digest)
    {
        return Err(expired());
    }
    let bound = object.get("g").ok_or_else(expired)?;
    let bound_generation: super::super::protocol::Generation =
        serde_json::from_value(bound.clone()).map_err(|_| expired())?;
    if bound_generation != *generation {
        return Err(expired());
    }
    let null_flag = object
        .get("nf")
        .and_then(Value::as_bool)
        .ok_or_else(expired)?;
    let id = object
        .get("id")
        .and_then(Value::as_i64)
        .ok_or_else(expired)?;
    if id <= 0 {
        return Err(expired());
    }
    let value = if null_flag {
        None
    } else {
        // Mirror of encode_cursor: "v" is the f64 bit pattern (u64), so the
        // decode never passes through serde_json's inexact float parse.
        Some(f64::from_bits(
            object
                .get("v")
                .and_then(Value::as_u64)
                .ok_or_else(expired)?,
        ))
    };
    Ok(SearchCursor {
        value,
        null_flag,
        id,
    })
}
