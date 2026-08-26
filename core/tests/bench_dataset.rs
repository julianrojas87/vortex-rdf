//! Golden values of the benchmark dataset generator, computed once and pinned
//! with identical literals in `js/test/bench-datasets.test.ts` and
//! `python/tests/test_bench_datasets.py`, so the three dashboard tabs keep
//! measuring the same data.

#[path = "../benches/support/dataset.rs"]
#[allow(dead_code)]
mod dataset;

use dataset::{
    Moduli, ObjectTerm, Pattern, WANT_GRAPHS, dataset_quad, moduli, object_term, terms_for,
};

const BASE: &str = "http://data.example.org";

#[test]
fn moduli_at_the_dashboard_scales_with_8_graphs() {
    assert_eq!(WANT_GRAPHS, 8);
    let small = moduli(32_768, 8);
    assert_eq!(
        small,
        Moduli {
            n_subj: 3277,
            n_pred: 32,
            n_obj: 16_387,
            n_graph: 9,
        }
    );
    assert_eq!(small.terms(), 19_705);
    let large = moduli(1_048_576, 8);
    assert_eq!(
        large,
        Moduli {
            n_subj: 104_858,
            n_pred: 33,
            n_obj: 524_291,
            n_graph: 17,
        }
    );
    assert_eq!(large.terms(), 629_199);
}

#[test]
fn nquads_spellings_of_quads_0_1_7_and_12345_at_32768_rows() {
    let m = moduli(32_768, 8);
    let spell = |i: usize| {
        let q = dataset_quad(i, m);
        // oxrdf's Display renders exactly the N-Quads terms.
        format!(
            "{} {} {} {} .",
            q.subject, q.predicate, q.object, q.graph_name
        )
    };
    assert_eq!(
        spell(0),
        format!(
            "<{BASE}/resource/2026/subject/000000000> <{BASE}/ontology/2026/property/0000> \"descriptive object value number 000000000\" <{BASE}/graph/2026/named/000000> ."
        )
    );
    assert_eq!(
        spell(1),
        format!(
            "<{BASE}/resource/2026/subject/000000001> <{BASE}/ontology/2026/property/0001> \"descriptive object value number 000000001\" <{BASE}/graph/2026/named/000001> ."
        )
    );
    assert_eq!(
        spell(7),
        format!(
            "<{BASE}/resource/2026/subject/000000007> <{BASE}/ontology/2026/property/0007> <{BASE}/resource/2026/object/000000007> <{BASE}/graph/2026/named/000007> ."
        )
    );
    assert_eq!(
        spell(12_345),
        format!(
            "<{BASE}/resource/2026/subject/000002514> <{BASE}/ontology/2026/property/0025> <{BASE}/resource/2026/object/000012345> <{BASE}/graph/2026/named/000006> ."
        )
    );
}

#[test]
fn object_0_is_a_literal_so_the_o_probe_binds_one() {
    match object_term(0) {
        ObjectTerm::Literal(value) => {
            assert_eq!(value, "descriptive object value number 000000000")
        }
        ObjectTerm::Iri(iri) => panic!("object 0 is an IRI: {iri}"),
    }
    assert_eq!(
        object_term(0).to_ntriples(),
        "\"descriptive object value number 000000000\""
    );
}

#[test]
fn quad_probes_bind_named_graph_0() {
    // The Rust generator always writes named graphs, so its `G`/`SPOG` probes
    // bind graph 0 (the Python generator returns no quad probes at graphs=1).
    let (_, _, _, g) = terms_for(Pattern::G);
    assert_eq!(
        g.expect("G binds a graph").to_string(),
        format!("<{BASE}/graph/2026/named/000000>")
    );
    let (s, p, o, g) = terms_for(Pattern::SPOG);
    assert!(s.is_some() && p.is_some() && o.is_some() && g.is_some());
}
