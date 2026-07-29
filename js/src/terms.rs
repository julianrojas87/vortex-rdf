//! RDF/JS term values ⇄ `oxrdf` terms: reading `termType`/`value`/
//! `language`/`datatype` off JS objects (or bare IRI strings) into owned,
//! validated terms.

use js_sys::Reflect;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use wasm_bindgen::prelude::*;

struct RawTerm {
    term_type: String,
    value: String,
    language: Option<String>,
    datatype_iri: Option<String>,
}

fn js_to_term_raw(val: JsValue) -> Option<RawTerm> {
    if val.is_null() || val.is_undefined() {
        return None;
    }
    if let Some(s) = val.as_string() {
        return Some(RawTerm {
            term_type: "NamedNode".into(),
            value: s,
            language: None,
            datatype_iri: None,
        });
    }
    let term_type = Reflect::get(&val, &"termType".into()).ok()?.as_string()?;
    let value = Reflect::get(&val, &"value".into()).ok()?.as_string()?;
    let language = Reflect::get(&val, &"language".into())
        .ok()
        .and_then(|v| v.as_string());
    let datatype_iri = Reflect::get(&val, &"datatype".into())
        .ok()
        .and_then(|dt| Reflect::get(&dt, &"value".into()).ok())
        .and_then(|v| v.as_string());
    Some(RawTerm {
        term_type,
        value,
        language,
        datatype_iri,
    })
}

pub(crate) fn js_to_term(val: JsValue) -> Option<Term> {
    let raw = js_to_term_raw(val)?;
    match raw.term_type.as_str() {
        "NamedNode" => NamedNode::new(raw.value).ok().map(Term::NamedNode),
        "BlankNode" => Some(Term::BlankNode(oxrdf::BlankNode::new_unchecked(raw.value))),
        "Literal" => {
            if let Some(l) = raw.language
                && !l.is_empty()
            {
                return oxrdf::Literal::new_language_tagged_literal(raw.value, l)
                    .ok()
                    .map(Term::Literal);
            }
            if let Some(dt_iri) = raw.datatype_iri
                && let Ok(dt_node) = NamedNode::new(dt_iri)
            {
                return Some(Term::Literal(oxrdf::Literal::new_typed_literal(
                    raw.value, dt_node,
                )));
            }
            Some(Term::Literal(oxrdf::Literal::new_simple_literal(raw.value)))
        }
        _ => None,
    }
}

pub(crate) fn js_to_subject(val: JsValue) -> Option<NamedOrBlankNode> {
    match js_to_term(val)? {
        Term::NamedNode(n) => Some(NamedOrBlankNode::NamedNode(n)),
        Term::BlankNode(b) => Some(NamedOrBlankNode::BlankNode(b)),
        _ => None,
    }
}

pub(crate) fn js_to_named_node(val: JsValue) -> Option<NamedNode> {
    match js_to_term(val)? {
        Term::NamedNode(n) => Some(n),
        _ => None,
    }
}

pub(crate) fn js_to_graph(val: JsValue) -> Option<GraphName> {
    if let Some(term) = js_to_term_raw(val) {
        match term.term_type.as_str() {
            "NamedNode" => Some(GraphName::NamedNode(NamedNode::new(term.value).ok()?)),
            "BlankNode" => Some(GraphName::BlankNode(oxrdf::BlankNode::new_unchecked(
                term.value,
            ))),
            "DefaultGraph" => Some(GraphName::DefaultGraph),
            _ => None,
        }
    } else {
        None
    }
}

pub(crate) fn js_to_quad(val: JsValue) -> Option<Quad> {
    let s = js_to_subject(Reflect::get(&val, &"subject".into()).ok()?)?;
    let p = js_to_named_node(Reflect::get(&val, &"predicate".into()).ok()?)?;
    let o = js_to_term(Reflect::get(&val, &"object".into()).ok()?)?;
    let g =
        js_to_graph(Reflect::get(&val, &"graph".into()).ok()?).unwrap_or(GraphName::DefaultGraph);
    Some(Quad::new(s, p, o, g))
}
