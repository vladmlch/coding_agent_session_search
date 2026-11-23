use super::cursor::{SearchCursor, decode_cursor, encode_cursor};
use crate::viewer_bridge::protocol::Generation;
use serde_json::json;

fn generation() -> Generation {
    Generation {
        canonical_revision: 7,
        annotation_revision: 3,
        projection_revision: 5,
        lexical_generation: 9,
    }
}

/// encode -> decode must be BIT-EXACT for every sort value. The keyset
/// after-predicate resumes inside tie groups via exact f64 equality, so
/// a bound even one ULP off the encoded key either strands the whole tie
/// suffix (bound low: `cargo` on the owner archive lost 302 of 1907
/// members — the decoded bound 1.590656280517578 never equals the scores
/// 1.5906562805175781) or re-emits ties forever (bound high: the cycling
/// the incumbent binary's walk showed). Decimal floats cannot be trusted
/// here because serde_json's float parse is not correctly rounded for
/// every f64, so `v` carries the raw bit pattern instead.
#[test]
fn cursor_value_roundtrips_bit_exactly() {
    let cur_gen = generation();
    let digest = "d473e0e4cf3a6b1c";
    // The f32-widened relevance scores the fast lane emits: exactly the
    // 17-significant-digit f64 class a decimal roundtrip can miss.
    let f32_widened: Vec<f32> = vec![
        1.5906563, 3.9335427, 27.335167, 29.436178, 1.5365977, 1.4536486, 0.0, 1043.9766,
    ];
    let mut values: Vec<f64> = f32_widened.iter().map(|v| f64::from(*v)).collect();
    // Millisecond epoch keys (created/activity sorts): exact in f64.
    values.push(1_762_000_000_123_f64);
    // Hostile f64s: if the encoding ever degrades to a decimal string,
    // these are the first shapes to lose a ULP or worse.
    values.extend([
        0.1 + 0.2,
        -2.75,
        f64::EPSILON,
        f64::MIN_POSITIVE,
        f64::MAX,
        f64::INFINITY,
        f64::NAN,
    ]);
    for v in values {
        for sort in ["relevance", "created_asc"] {
            let cursor = SearchCursor {
                value: Some(v),
                null_flag: false,
                id: 42,
            };
            let raw = encode_cursor(sort, &cursor, &cur_gen, digest);
            let decoded = decode_cursor(&raw, sort, &cur_gen, digest)
                .unwrap_or_else(|e| panic!("sort={sort} value={v:?}: {e:?}"));
            assert!(!decoded.null_flag, "sort={sort} value={v:?}");
            assert_eq!(decoded.id, 42, "sort={sort} value={v:?}");
            let got = decoded.value.expect("value must decode present");
            assert_eq!(
                got.to_bits(),
                v.to_bits(),
                "sort={sort}: decoded bits differ from encoded bits"
            );
        }
    }
}

/// Null sort keys (sessions without created/activity metadata) must
/// roundtrip as null_flag, never as a numeric value.
#[test]
fn cursor_null_value_roundtrips() {
    let cur_gen = generation();
    let cursor = SearchCursor {
        value: None,
        null_flag: true,
        id: 7,
    };
    let raw = encode_cursor("created_asc", &cursor, &cur_gen, "digest");
    let decoded = decode_cursor(&raw, "created_asc", &cur_gen, "digest").unwrap();
    assert!(decoded.null_flag);
    assert_eq!(decoded.value, None);
    assert_eq!(decoded.id, 7);
}

/// ss1 cursors (decimal-float `v`) are the lossy format this fix retires:
/// they must refuse honestly as cursor_expired, never silently decode a
/// 1-ULP-off bound. A float-typed `v` inside an ss2 cursor is refused the
/// same way — the bit-pattern lane only accepts integers.
#[test]
fn legacy_and_forged_cursors_refuse() {
    use crate::viewer_bridge::protocol::Refusal;
    let refusal_kind = |err: anyhow::Error| -> &'static str {
        err.downcast_ref::<Refusal>()
            .map(|refusal| refusal.kind)
            .unwrap_or("non-refusal error")
    };
    let cur_gen = generation();
    let digest = "d473e0e4cf3a6b1c";
    let legacy = json!({
        "k": "ss1",
        "s": "relevance",
        "nf": false,
        "v": 1.5906562805175781_f64,
        "id": 42,
        "g": cur_gen,
        "q": digest,
    })
    .to_string();
    let err = decode_cursor(&legacy, "relevance", &cur_gen, digest).unwrap_err();
    assert_eq!(
        refusal_kind(err),
        "cursor_expired",
        "ss1 (decimal-float v) must refuse as cursor_expired"
    );

    let forged = json!({
        "k": "ss2",
        "s": "relevance",
        "nf": false,
        "v": 1.5906562805175781_f64,
        "id": 42,
        "g": cur_gen,
        "q": digest,
    })
    .to_string();
    let err = decode_cursor(&forged, "relevance", &cur_gen, digest).unwrap_err();
    assert_eq!(
        refusal_kind(err),
        "cursor_expired",
        "float-typed v inside an ss2 cursor must refuse as cursor_expired"
    );

    // Bound integrity: a different query digest, a different sort, or a
    // different generation must never resume another query's walk.
    let cursor = SearchCursor {
        value: Some(1.5_f64),
        null_flag: false,
        id: 42,
    };
    let raw = encode_cursor("relevance", &cursor, &cur_gen, digest);
    assert!(decode_cursor(&raw, "relevance", &cur_gen, "other-digest").is_err());
    assert!(decode_cursor(&raw, "created_asc", &cur_gen, digest).is_err());
    let mut other = cur_gen;
    other.lexical_generation += 1;
    assert!(decode_cursor(&raw, "relevance", &other, digest).is_err());
}
