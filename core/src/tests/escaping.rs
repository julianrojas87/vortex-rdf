//! Literal escaping: the store's columns hold N-Triples *escaped* lexical
//! forms, so every decode path has to unescape, and every structure scan has
//! to honour escapes rather than search the whole string for `^^` or `@`.

use super::*;
use crate::common::terms::{get_as_term, parse_term, parse_term_checked};
use crate::store::RawQuad;
use std::collections::HashMap;

/// Literals whose lexical value either needs escaping when serialized or
/// contains a sequence that looks like literal structure (`^^`, `"@`).
fn escaped_literal_cases() -> Vec<Term> {
    let dt = NamedNode::new("http://example.org/dt").unwrap();
    vec![
        Term::Literal(Literal::new_simple_literal("with \"quotes\" inside")),
        Term::Literal(Literal::new_simple_literal("back\\slash")),
        Term::Literal(Literal::new_simple_literal("trailing backslash \\")),
        Term::Literal(Literal::new_simple_literal("line\nbreak")),
        Term::Literal(Literal::new_simple_literal("carriage\rreturn")),
        Term::Literal(Literal::new_simple_literal("tab\there")),
        Term::Literal(Literal::new_simple_literal(
            "caret^^<http://example.org/nope> inside",
        )),
        Term::Literal(Literal::new_simple_literal("say \"hi\"@home")),
        Term::Literal(Literal::new_simple_literal("emoji \u{1F600} tail")),
        Term::Literal(Literal::new_language_tagged_literal("q\"x", "en").unwrap()),
        Term::Literal(Literal::new_language_tagged_literal("say \"hi\"@home", "en").unwrap()),
        Term::Literal(Literal::new_language_tagged_literal("back\\slash", "en-GB").unwrap()),
        Term::Literal(Literal::new_typed_literal("a \"b\" ^^ c", dt.clone())),
        Term::Literal(Literal::new_typed_literal("back\\slash\nand a newline", dt)),
    ]
}

fn subject(i: usize) -> NamedOrBlankNode {
    NamedOrBlankNode::NamedNode(NamedNode::new(format!("http://example.org/s{:02}", i)).unwrap())
}

fn escaped_literal_quads() -> Vec<Quad> {
    escaped_literal_cases()
        .into_iter()
        .enumerate()
        .map(|(i, o)| {
            Quad::new(
                subject(i),
                NamedNode::new("http://example.org/p").unwrap(),
                o,
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

async fn run_escaped_literal_roundtrip(layout: LayoutStrategy, indexes: Vec<IndexType>) {
    let quads = escaped_literal_quads();

    let arr = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        layout,
        indexes,
    )
    .await
    .expect("build failed");
    let store = VortexRdfStore::from_built(arr).unwrap();

    let decoded = store.quads_vec().await.unwrap();
    assert_eq!(decoded.len(), quads.len());
    let mut by_subject: HashMap<String, Term> = decoded
        .into_iter()
        .map(|q| (q.subject.to_string(), q.object))
        .collect();

    for q in &quads {
        let object = by_subject
            .remove(&q.subject.to_string())
            .unwrap_or_else(|| panic!("{:?}: missing row for {}", layout, q.subject));
        assert_eq!(object, q.object, "{:?}: object not round-tripped", layout);

        let matched = store
            .match_pattern(None, None, Some(&q.object), None)
            .await
            .unwrap();
        assert_eq!(
            matched.size().await.unwrap(),
            1,
            "{:?}: match by {} did not select exactly its row",
            layout,
            q.object
        );
        let rows = matched.quads_vec().await.unwrap();
        assert_eq!(rows[0].subject, q.subject);
        assert_eq!(rows[0].object, q.object);
    }
}

#[tokio::test]
async fn escaped_literals_roundtrip_default_layout() {
    run_escaped_literal_roundtrip(LayoutStrategy::Default, vec![]).await;
}

#[tokio::test]
async fn escaped_literals_roundtrip_dictionary_layout() {
    run_escaped_literal_roundtrip(LayoutStrategy::Dictionary, vec![]).await;
}

#[tokio::test]
async fn escaped_literals_roundtrip_typed_object_layout() {
    run_escaped_literal_roundtrip(LayoutStrategy::TypedObject, vec![]).await;
}

/// The object index stores the serialized (escaped) object term, so an
/// index-routed match has to agree with the scan on the same spelling.
#[tokio::test]
async fn escaped_literals_roundtrip_with_object_index() {
    run_escaped_literal_roundtrip(
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    run_escaped_literal_roundtrip(
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
}

#[test]
fn trusted_and_checked_parses_agree_on_escaped_literals() {
    for term in escaped_literal_cases() {
        let s = term.to_string();
        assert_eq!(parse_term(&s).unwrap(), term, "trusted parse of {}", s);
        assert_eq!(
            parse_term_checked(&s).unwrap(),
            term,
            "checked parse of {}",
            s
        );
        assert_eq!(get_as_term(&s).unwrap(), term, "get_as_term of {}", s);
    }
}

#[test]
fn structure_scan_ignores_suffix_lookalikes_inside_the_value() {
    // `"@` and `^^` inside the value must not be read as structure: both
    // parses see a simple literal, and the checked one does not error.
    for (serialized, value) in [
        ("\"say \\\"hi\\\"@home\"", "say \"hi\"@home"),
        ("\"a ^^ b\"", "a ^^ b"),
        (
            "\"\\\"^^<http://example.org/nope>\"",
            "\"^^<http://example.org/nope>",
        ),
    ] {
        let expected = Term::Literal(Literal::new_simple_literal(value));
        assert_eq!(parse_term(serialized).unwrap(), expected);
        assert_eq!(parse_term_checked(serialized).unwrap(), expected);
    }
}

#[test]
fn escape_sequences_decode_in_both_paths() {
    for (serialized, value) in [
        ("\"a\\u0041b\"", "aAb"),
        ("\"\\U0001F600\"", "\u{1F600}"),
        ("\"\\b\\f\"", "\u{8}\u{c}"),
        ("\"\\'\"", "'"),
        ("\"\\t\\n\\r\"", "\t\n\r"),
        ("\"\\\\\\\"\"", "\\\""),
    ] {
        let expected = Term::Literal(Literal::new_simple_literal(value));
        assert_eq!(parse_term(serialized).unwrap(), expected, "{}", serialized);
        assert_eq!(
            parse_term_checked(serialized).unwrap(),
            expected,
            "{}",
            serialized
        );
    }
}

#[test]
fn escaped_suffixes_are_read_from_after_the_closing_quote() {
    let dt = NamedNode::new("http://example.org/dt").unwrap();
    assert_eq!(
        parse_term("\"a\\\"b\"^^<http://example.org/dt>").unwrap(),
        Term::Literal(Literal::new_typed_literal("a\"b", dt))
    );
    assert_eq!(
        parse_term("\"a\\\"b\"@en").unwrap(),
        Term::Literal(Literal::new_language_tagged_literal("a\"b", "en").unwrap())
    );
}

/// A quote closes the literal only when the backslash run directly before it
/// has even length — the property the closing-quote scan is built on.
#[test]
fn backslash_run_parity_decides_the_closing_quote() {
    // Serialized form -> the value it denotes, over runs of length 1..=3
    // ending at a quote.
    let cases = [
        ("\"a\\\\\"", "a\\"),           // "a\\"      -> a\
        ("\"a\\\"b\"", "a\"b"),         // "a\"b"     -> a"b
        ("\"a\\\\\\\"b\"", "a\\\"b"),   // "a\\\"b"   -> a\"b
        ("\"\\\\\\\\\"", "\\\\"),       // "\\\\"     -> \\
        ("\"\\\\\\\\\\\"\"", "\\\\\""), // "\\\\\""   -> \\"
    ];
    for (serialized, value) in cases {
        let expected = Term::Literal(Literal::new_simple_literal(value));
        assert_eq!(parse_term(serialized).unwrap(), expected, "{}", serialized);
        assert_eq!(
            parse_term_checked(serialized).unwrap(),
            expected,
            "{}",
            serialized
        );
        // And the value must render back to exactly the form we started from.
        assert_eq!(expected.to_string(), serialized);
    }
}

#[test]
fn stored_object_strings_survive_a_parse_and_reserialize() {
    for term in escaped_literal_cases() {
        let stored = term.to_string();
        let quad = Quad::new(
            subject(0),
            NamedNode::new("http://example.org/p").unwrap(),
            parse_term(&stored).unwrap(),
            GraphName::DefaultGraph,
        );
        assert_eq!(RawQuad::from_quad(&quad).o, stored);
    }
}

#[test]
fn malformed_literals_are_lenient_when_trusted_and_rejected_when_checked() {
    for s in ["\"unterminated", "\"a\" trailing", "\"a\"\\"] {
        // Trusted decode is infallible: it must not panic and must yield a term.
        assert!(matches!(parse_term(s).unwrap(), Term::Literal(_)), "{}", s);
        assert!(parse_term_checked(s).is_err(), "{}", s);
    }
}
