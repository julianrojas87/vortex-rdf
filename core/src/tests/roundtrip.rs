use super::*;

// ─── 1) Foundational roundtrip tests ───────────────────────────────────

#[tokio::test]
async fn test_roundtrip() {
    let quad = make_quad(
        "http://example.org/s",
        "http://example.org/p",
        "hello",
        GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap()),
    );

    let arr = VortexRdfStore::build_vortex_array(quad_stream(vec![quad.clone()]))
        .await
        .expect("build failed");
    let store = VortexRdfStore::new(arr).unwrap();

    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].subject.to_string(), quad.subject.to_string());
    assert_eq!(decoded[0].predicate.to_string(), quad.predicate.to_string());
    assert_eq!(decoded[0].object.to_string(), quad.object.to_string());
    assert_eq!(
        decoded[0].graph_name.to_string(),
        quad.graph_name.to_string()
    );
}

async fn run_builder_roundtrip<B: VortexArrayBuilder>() {
    let quad = make_quad(
        "http://example.org/s",
        "http://example.org/p",
        "hello",
        GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap()),
    );

    let arr = VortexRdfStore::build_vortex_array_with_builder::<B>(
        quad_stream(vec![quad.clone()]),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .expect("build failed");

    let store = VortexRdfStore::from_built(arr).unwrap();
    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].subject.to_string(), quad.subject.to_string());
    assert_eq!(decoded[0].predicate.to_string(), quad.predicate.to_string());
    assert_eq!(decoded[0].object.to_string(), quad.object.to_string());
    assert_eq!(
        decoded[0].graph_name.to_string(),
        quad.graph_name.to_string()
    );
}

#[tokio::test]
async fn test_sorted_in_memory() {
    run_builder_roundtrip::<SortedInMemoryBuilder>().await;
}
#[tokio::test]
async fn test_sorted_stream() {
    run_builder_roundtrip::<SortedStreamBuilder>().await;
}
#[tokio::test]
async fn test_unsorted_stream() {
    run_builder_roundtrip::<UnsortedStreamBuilder>().await;
}
