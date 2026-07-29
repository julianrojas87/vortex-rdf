"""BGP pushdown must be observationally identical to rdflib's default
evaluator — every query shape is run both ways and compared exactly."""

import pytest
from rdflib import Graph, Literal, URIRef

from vortex_rdflib import VortexStore, register_sparql_pushdown, unregister_sparql_pushdown
from vortex_rdflib._native import serialize_rdf

FIXTURE_NT = """\
<http://ex.org/alice> <http://ex.org/name> "Alice" .
<http://ex.org/alice> <http://ex.org/knows> <http://ex.org/bob> .
<http://ex.org/alice> <http://ex.org/knows> <http://ex.org/carol> .
<http://ex.org/bob> <http://ex.org/name> "Bob"@en .
<http://ex.org/bob> <http://ex.org/knows> <http://ex.org/carol> .
<http://ex.org/bob> <http://ex.org/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex.org/carol> <http://ex.org/name> "Carol" .
<http://ex.org/carol> <http://ex.org/likes> <http://ex.org/carol> .
_:b0 <http://ex.org/name> "Anon" .
_:b0 <http://ex.org/knows> <http://ex.org/alice> .
"""

QUERIES = [
    # single patterns
    'SELECT ?s ?o WHERE { ?s <http://ex.org/name> ?o }',
    'SELECT ?p ?o WHERE { <http://ex.org/bob> ?p ?o }',
    'SELECT * WHERE { ?s ?p ?o }',
    # chain join (object of one TP is subject of the next)
    """SELECT ?n WHERE {
        <http://ex.org/alice> <http://ex.org/knows> ?x .
        ?x <http://ex.org/name> ?n }""",
    # star join
    """SELECT ?x ?n ?a WHERE {
        ?x <http://ex.org/name> ?n .
        ?x <http://ex.org/age> ?a }""",
    # three-pattern chain
    """SELECT ?a ?c WHERE {
        ?a <http://ex.org/knows> ?b .
        ?b <http://ex.org/knows> ?c .
        ?c <http://ex.org/name> ?n }""",
    # repeated variable inside one pattern (s == o)
    'SELECT ?x WHERE { ?x <http://ex.org/likes> ?x }',
    # ground pattern acting as a filter alongside a variable pattern
    """SELECT ?n WHERE {
        <http://ex.org/bob> <http://ex.org/knows> <http://ex.org/carol> .
        <http://ex.org/carol> <http://ex.org/name> ?n }""",
    # ground pattern that does NOT match: must kill all solutions
    """SELECT ?n WHERE {
        <http://ex.org/carol> <http://ex.org/knows> <http://ex.org/bob> .
        ?s <http://ex.org/name> ?n }""",
    # cross product (no shared variables)
    """SELECT ?a ?b WHERE {
        <http://ex.org/alice> <http://ex.org/name> ?a .
        <http://ex.org/bob> <http://ex.org/name> ?b }""",
    # bnode in the query pattern (acts as a variable)
    'SELECT ?n WHERE { [] <http://ex.org/name> ?n }',
    # OPTIONAL: inner BGP evaluated under outer bindings
    """SELECT ?x ?a WHERE {
        ?x <http://ex.org/name> ?n .
        OPTIONAL { ?x <http://ex.org/age> ?a } }""",
    # UNION
    """SELECT ?o WHERE {
        { <http://ex.org/alice> <http://ex.org/knows> ?o }
        UNION { ?o <http://ex.org/likes> ?o } }""",
    # FILTER over joined bindings
    """SELECT ?x WHERE {
        ?x <http://ex.org/name> ?n .
        FILTER(lang(?n) = "en") }""",
    # VALUES feeding bindings into the BGP
    """SELECT ?n WHERE {
        VALUES ?x { <http://ex.org/bob> <http://ex.org/carol> }
        ?x <http://ex.org/name> ?n }""",
    # literal join value propagating into subject position (unsatisfiable leg)
    """SELECT ?n WHERE {
        ?s <http://ex.org/name> ?n .
        ?n <http://ex.org/name> ?m }""",
    # zero-match pattern
    'SELECT ?s WHERE { ?s <http://ex.org/nothing> ?o }',
    # DISTINCT + ORDER BY on top of a join
    """SELECT DISTINCT ?b WHERE {
        ?a <http://ex.org/knows> ?b .
        ?b <http://ex.org/name> ?n } ORDER BY ?b""",
]


@pytest.fixture(scope="module")
def dict_vortex(tmp_path_factory):
    d = tmp_path_factory.mktemp("pushdown")
    nt = d / "fixture.nt"
    nt.write_text(FIXTURE_NT)
    out = d / "fixture.vortex"
    serialize_rdf(str(nt), str(out), layout="dictionary")
    return out


def run(graph, sparql):
    return sorted(tuple(row) for row in graph.query(sparql))


@pytest.mark.parametrize("sparql", QUERIES)
def test_pushdown_equals_default_evaluator(dict_vortex, sparql):
    graph = Graph(store=VortexStore(str(dict_vortex)))
    register_sparql_pushdown()
    with_pushdown = run(graph, sparql)
    try:
        unregister_sparql_pushdown()
        without_pushdown = run(graph, sparql)
    finally:
        register_sparql_pushdown()
    assert with_pushdown == without_pushdown


def test_pushdown_is_actually_used(dict_vortex, monkeypatch):
    import vortex_rdflib.pushdown as pd

    calls = []
    original = pd._solve_bgp
    monkeypatch.setattr(pd, "_solve_bgp", lambda *a: calls.append(1) or original(*a))
    graph = Graph(store=VortexStore(str(dict_vortex)))
    register_sparql_pushdown()
    rows = run(graph, "SELECT ?s ?o WHERE { ?s <http://ex.org/name> ?o }")
    assert len(rows) == 4
    assert calls, "pushdown hook was not invoked"


def test_non_vortex_graphs_unaffected(dict_vortex):
    register_sparql_pushdown()
    g = Graph()
    g.add((URIRef("http://ex.org/a"), URIRef("http://ex.org/name"), Literal("A")))
    rows = run(g, "SELECT ?n WHERE { ?s <http://ex.org/name> ?n }")
    assert rows == [(Literal("A"),)]


def test_string_fallback_when_code_path_disabled(dict_vortex, monkeypatch):
    monkeypatch.setenv("VORTEX_RDF_DISABLE_CODE_PATH", "1")
    graph = Graph(store=VortexStore(str(dict_vortex)))
    rows = run(
        graph,
        """SELECT ?n WHERE {
            <http://ex.org/alice> <http://ex.org/knows> ?x .
            ?x <http://ex.org/name> ?n }""",
    )
    assert {row[0] for row in rows} == {Literal("Bob", lang="en"), Literal("Carol")}
