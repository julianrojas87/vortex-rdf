//! Warm-path layer split over a real serialized store: median match-only and
//! match+materialize times per pattern, on the file-backed and in-memory
//! (resident-adopted) forms, plus a low-cardinality (dict-encoded) fixture.
//! Instrumentation only — not part of the bench suite.

use std::time::Instant;

use oxrdf::{Literal, NamedNode, NamedOrBlankNode, Term};
use vortex_rdf_core::{BuilderStrategy, IndexType, LayoutStrategy, RawQuad, VortexRdfStore};

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(f64::total_cmp);
    xs[xs.len() / 2]
}

async fn run_case(
    store: &VortexRdfStore,
    label: &str,
    s: Option<&NamedOrBlankNode>,
    p: Option<&NamedNode>,
    o: Option<&Term>,
) {
    let mut match_us = Vec::new();
    let mut vec_us = Vec::new();
    let mut rows = 0;
    for i in 0..220 {
        let t = Instant::now();
        let view = store.match_pattern(s, p, o, None).await.expect("match");
        let m = t.elapsed().as_secs_f64() * 1e6;
        let t = Instant::now();
        let quads = view.quads_vec().await.expect("vec");
        let v = t.elapsed().as_secs_f64() * 1e6;
        rows = quads.len();
        if i >= 20 {
            match_us.push(m);
            vec_us.push(v);
        }
    }
    println!(
        "{label:24} rows={rows:6} match_median={:9.1}us quads_vec_median={:9.1}us",
        median(match_us),
        median(vec_us)
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let path = std::env::args().nth(1).expect("artifact path");

    let file = VortexRdfStore::from_file(&path).await.expect("open file");
    let bytes = std::fs::read(&path).expect("read artifact");
    let adopted = VortexRdfStore::from_bytes_owned(bytes)
        .await
        .expect("adopt");
    let t = Instant::now();
    let mem = VortexRdfStore::from_parts(adopted.to_serializable_parts().await.expect("parts"))
        .expect("from_parts");
    println!(
        "from_parts (resident adoption): {:.1}ms",
        t.elapsed().as_secs_f64() * 1e3
    );

    let s = NamedOrBlankNode::NamedNode(
        NamedNode::new("http://data.example.org/resource/2026/subject/000000000").unwrap(),
    );
    let p = NamedNode::new("http://data.example.org/ontology/2026/property/0000").unwrap();
    let o = Term::Literal(Literal::new_simple_literal(
        "descriptive object value number 000000000",
    ));
    for (label, store) in [("file", &file), ("mem", &mem)] {
        run_case(store, &format!("{label} S"), Some(&s), None, None).await;
        run_case(store, &format!("{label} O"), None, None, Some(&o)).await;
        run_case(store, &format!("{label} PO"), None, Some(&p), Some(&o)).await;
        run_case(store, &format!("{label} SPO"), Some(&s), Some(&p), Some(&o)).await;
    }

    // Low-cardinality object fixture: dict-encodes on the wire, so adoption
    // and the PO/O read paths exercise the dictionary-shaped route.
    let raws: Vec<Result<RawQuad, vortex_rdf_core::VortexRdfError>> = (0..100_000)
        .map(|i| {
            Ok(RawQuad {
                s: format!("<http://example.org/s{:06}>", i / 4),
                p: format!("<http://example.org/p{}>", i % 7),
                o: format!("\"object {}\"", i % 13),
                g: String::new(),
            })
        })
        .collect();
    let built = BuilderStrategy::SortedInMemory
        .build_vortex_array(
            Box::new(futures::stream::iter(raws)),
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByCopy],
        )
        .await
        .expect("build");
    let canonical = VortexRdfStore::from_built(built).expect("from_built");
    let bytes = canonical.to_bytes().await.expect("bytes");
    let lean = VortexRdfStore::from_bytes(&bytes).await.expect("lean");
    let t = Instant::now();
    let dict_mem = VortexRdfStore::from_parts(lean.to_serializable_parts().await.expect("parts"))
        .expect("from_parts");
    println!(
        "dict fixture from_parts: {:.2}ms",
        t.elapsed().as_secs_f64() * 1e3
    );
    let p4 = NamedNode::new("http://example.org/p4").unwrap();
    let o7 = Term::Literal(Literal::new_simple_literal("object 7"));
    run_case(&dict_mem, "dictfix PO", None, Some(&p4), Some(&o7)).await;
    run_case(&dict_mem, "dictfix O", None, None, Some(&o7)).await;
}
