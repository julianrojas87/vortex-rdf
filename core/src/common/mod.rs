//! Textual RDF vocabulary, shared by the store's decode paths and every
//! binding: term parsing/reconstruction ([`terms`]) and format-name
//! resolution ([`formats`]).
//!
//! The boundary is the reason this module can be named `common` without
//! becoming a grab bag: an item belongs here only when it is about RDF *text*
//! and nothing about Vortex. Anything that touches arrays, layouts, or the
//! container format belongs in [`store`](crate::store) or [`io`](crate::io).

pub mod formats;
pub mod terms;
