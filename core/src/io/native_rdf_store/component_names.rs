//! Stable persisted names for transparent native RDF store components.

pub const QUAD_SOURCE: &str = "quad-source";
pub const DICTIONARY_ID_TO_TERM: &str = "dictionary.id-to-term";
pub const DICTIONARY_TERM_TO_ID: &str = "dictionary.term-to-id";
pub const DICTIONARY_TERM_DIRECTORY: &str = "dictionary.term-directory";

pub const INDEX_SUBJECT_RANGES: &str = "index.subject.ranges";
pub const INDEX_PREDICATE_RUNS: &str = "index.predicate.runs";
pub const INDEX_PREDICATE_EXACT_DIRECTORY: &str = "index.predicate.exact-ranges.directory";
pub const INDEX_PREDICATE_EXACT_PAYLOAD: &str = "index.predicate.exact-ranges.payload";
pub const INDEX_PREDICATE_OBJECT_PARTITIONS: &str = "index.predicate-object.predicate-partitions";
pub const INDEX_PREDICATE_OBJECT_EXACT_DIRECTORY: &str =
    "index.predicate-object.exact-ranges.directory";
pub const INDEX_PREDICATE_OBJECT_EXACT_PAYLOAD: &str =
    "index.predicate-object.exact-ranges.payload";
pub const INDEX_OBJECT_EXACT_DIRECTORY: &str = "index.object.exact-ranges.directory";
pub const INDEX_OBJECT_EXACT_PAYLOAD: &str = "index.object.exact-ranges.payload";
