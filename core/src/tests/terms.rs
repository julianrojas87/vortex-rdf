//! Term parsing from serialized N-Triples strings.

use crate::common::terms::{get_as_term, parse_term};
use oxrdf::{Literal, NamedNode, Term};

#[test]
fn parse_term_simple_literal() {
    assert_eq!(
        parse_term("\"Alice\"").unwrap(),
        Term::Literal(Literal::new_simple_literal("Alice"))
    );
}

#[test]
fn parse_term_language_tagged_literal() {
    assert_eq!(
        parse_term("\"Bob\"@en").unwrap(),
        Term::Literal(Literal::new_language_tagged_literal("Bob", "en").unwrap())
    );
}

#[test]
fn parse_term_typed_literal() {
    let dt = NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap();
    assert_eq!(
        parse_term("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>").unwrap(),
        Term::Literal(Literal::new_typed_literal("42", dt))
    );
}

#[test]
fn parse_term_named_and_blank_nodes() {
    assert_eq!(
        parse_term("<http://example.org/x>").unwrap(),
        Term::NamedNode(NamedNode::new("http://example.org/x").unwrap())
    );
    // Bare IRIs (no angle brackets) are accepted, e.g. from CLI arguments.
    assert_eq!(
        parse_term("http://example.org/x").unwrap(),
        Term::NamedNode(NamedNode::new("http://example.org/x").unwrap())
    );
    assert!(matches!(
        parse_term("_:b0").unwrap(),
        Term::BlankNode(b) if b.as_str() == "b0"
    ));
}

#[test]
fn parse_term_agrees_with_get_as_term_on_literals() {
    for s in [
        "\"plain\"",
        "\"tagged\"@en-GB",
        "\"7\"^^<http://www.w3.org/2001/XMLSchema#byte>",
        "\"an @ inside\"",
    ] {
        assert_eq!(parse_term(s).unwrap(), get_as_term(s).unwrap());
    }
}
