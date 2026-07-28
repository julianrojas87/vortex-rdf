//! Mock-data generation for benchmarks and tests.

use crate::error::Result;
use crate::store::RawQuad;

use futures::{Stream, stream};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};

/// Helper function to generate a stream of mock RDF quads for benchmark and test workflows.
/// Generates triples evenly distributed across 10 named graphs.
pub fn generate_rdf_data_stream(size: usize) -> impl Stream<Item = Result<RawQuad>> {
    const EX: &str = "http://example.org/";
    const NUM_GRAPHS: u64 = 10;

    stream::iter((0..size).map(|i| {
        let subject =
            NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(format!("{}subject/{}", EX, i)));
        let predicate = NamedNode::new_unchecked(format!("{}predicate/{}", EX, i % 100));
        let object = Term::NamedNode(NamedNode::new_unchecked(format!("{}object/{}", EX, i % 50)));
        let graph = GraphName::NamedNode(NamedNode::new_unchecked(format!(
            "{}graph/{}",
            EX,
            (i as u64) % NUM_GRAPHS
        )));

        Ok(RawQuad::from_quad(&Quad::new(
            subject, predicate, object, graph,
        )))
    }))
}
