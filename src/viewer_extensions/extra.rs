//! Strict JSON-compatible msgpack/JSON decoding with preserved quality diagnostics.
use std::fmt;
use std::io::Cursor;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Number, Value};

const MAX_NODES: usize = 100_000;
const MAX_EXTRA_DEPTH: usize = 58;
const HISTORICAL: &str = "__cass_historical_raw_json__";

struct StrictValue(Value);
struct StrictKey(String);

impl<'de> Deserialize<'de> for StrictKey {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct KeyVisitor;
        impl Visitor<'_> for KeyVisitor {
            type Value = StrictKey;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON string key")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(StrictKey(value.to_owned()))
            }
            fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(StrictKey(value))
            }
        }
        decoder.deserialize_any(KeyVisitor)
    }
}

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictValue;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("unambiguous finite JSON data")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }
            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                self.visit_unit()
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(StrictValue(Value::Bool(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                Number::from_f64(v)
                    .map(|v| StrictValue(Value::Number(v)))
                    .ok_or_else(|| E::custom("nonfinite_number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(StrictValue(Value::String(v.to_owned())))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(StrictValue(Value::String(v)))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                if seq.size_hint().is_some_and(|size| size > MAX_NODES) {
                    return Err(de::Error::custom("node_limit"));
                }
                let mut out = Vec::new();
                while let Some(StrictValue(value)) = seq.next_element()? {
                    if out.len() >= MAX_NODES {
                        return Err(de::Error::custom("node_limit"));
                    }
                    out.push(value);
                }
                Ok(StrictValue(Value::Array(out)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                if map.size_hint().is_some_and(|size| size > MAX_NODES) {
                    return Err(de::Error::custom("node_limit"));
                }
                let mut out = Map::new();
                while let Some((StrictKey(key), StrictValue(value))) =
                    map.next_entry::<StrictKey, StrictValue>()?
                {
                    if out.len() >= MAX_NODES || out.contains_key(&key) {
                        return Err(de::Error::custom("ambiguous_or_oversized_map"));
                    }
                    out.insert(key, value);
                }
                Ok(StrictValue(Value::Object(out)))
            }
        }
        decoder.deserialize_any(StrictVisitor)
    }
}

fn bounded(value: &Value) -> bool {
    let mut queue = vec![(value, 0_usize)];
    let mut nodes = 0;
    while let Some((item, depth)) = queue.pop() {
        nodes += 1;
        if depth > MAX_EXTRA_DEPTH || nodes > MAX_NODES {
            return false;
        }
        match item {
            Value::Array(v) => queue.extend(v.iter().map(|x| (x, depth + 1))),
            Value::Object(v) => queue.extend(v.values().map(|x| (x, depth + 1))),
            _ => (),
        }
    }
    true
}

fn json_value(text: &str) -> Result<Value, String> {
    // The default JSON decoder has its own bounded recursion limit; reject a
    // shallower profile breach before it allocates the tree.
    let mut depth = 0_usize;
    let (mut quoted, mut escaped) = (false, false);
    for byte in text.bytes() {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else if byte == b'"' {
            quoted = true;
        } else if byte == b'[' || byte == b'{' {
            depth += 1;
            if depth > MAX_EXTRA_DEPTH {
                return Err("nesting_limit".into());
            }
        } else if byte == b']' || byte == b'}' {
            depth = depth.saturating_sub(1);
        }
    }
    let StrictValue(value) = serde_json::from_str(text).map_err(|e| e.to_string())?;
    if !bounded(&value) {
        return Err("node_limit".into());
    }
    Ok(value)
}

fn binary_value(bytes: &[u8]) -> Result<Value, String> {
    let mut decoder = rmp_serde::Deserializer::new(Cursor::new(bytes));
    decoder.set_max_depth(MAX_EXTRA_DEPTH);
    let StrictValue(value) = StrictValue::deserialize(&mut decoder).map_err(|e| e.to_string())?;
    if decoder.position() != bytes.len() as u64 || !bounded(&value) {
        return Err("trailing_msgpack_or_node_limit".into());
    }
    Ok(value)
}

#[derive(Debug, Serialize)]
pub struct DecodedExtra {
    pub value: Value,
    pub origin: &'static str,
    pub complete: bool,
    pub issues: Vec<&'static str>,
}

fn finish(
    mut value: Value,
    mut origin: &'static str,
    mut issues: Vec<&'static str>,
) -> DecodedExtra {
    for index in 0..=4 {
        let Some(map) = value
            .as_object()
            .filter(|map| map.len() == 1 && map.contains_key(HISTORICAL))
        else {
            break;
        };
        origin = "historical";
        if index == 4 {
            issues.push("historical_wrapper_limit");
            break;
        }
        let Some(text) = map[HISTORICAL].as_str() else {
            issues.push("invalid_historical_wrapper");
            break;
        };
        match json_value(text) {
            Ok(decoded) => value = decoded,
            Err(_) => {
                issues.push("invalid_historical_json");
                break;
            }
        }
    }
    if value.is_null() {
        issues.push("missing_extra");
    } else if !value.is_object() && !value.is_array() {
        issues.push("unexpected_extra_shape");
    }
    DecodedExtra {
        value,
        origin,
        complete: issues.is_empty(),
        issues,
    }
}

pub fn decode(binary: Option<Vec<u8>>, text: Option<String>) -> DecodedExtra {
    let mut issues = Vec::new();
    if let Some(bytes) = binary {
        match binary_value(&bytes) {
            Ok(value) => return finish(value, "msgpack", issues),
            Err(_) => issues.push("invalid_msgpack"),
        }
    }
    if let Some(text) = text {
        match json_value(&text) {
            Ok(value) => return finish(value, "json", issues),
            Err(_) => issues.push("invalid_json"),
        }
    }
    finish(Value::Null, "absent", issues)
}
