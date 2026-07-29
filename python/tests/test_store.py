import pytest
from rdflib import BNode, Graph, Literal, URIRef

from conftest import LAYOUTS

from vortex_rdflib import VortexStore

FOAF_NAME = URIRef("http://xmlns.com/foaf/0.1/name")
ALICE = URIRef("http://ex.org/alice")
BOB = URIRef("http://ex.org/bob")


@pytest.fixture(params=LAYOUTS)
def graph(vortex_files, request):
    return Graph(store=VortexStore(str(vortex_files[request.param])))


def test_len(graph):
    assert len(graph) == 5


def test_triples_pattern(graph):
    names = list(graph.triples((None, FOAF_NAME, None)))
    assert len(names) == 3
    assert (ALICE, FOAF_NAME, Literal("Alice")) in names

    bob = set(graph.triples((BOB, None, None)))
    assert bob == {
        (BOB, FOAF_NAME, Literal("Bob", lang="en")),
        (BOB, URIRef("http://ex.org/age"), Literal(42)),
    }


def test_join_position_guard(graph):
    # A literal propagated into subject position is unsatisfiable, not an error.
    assert list(graph.triples((Literal("Alice"), None, None))) == []
    assert list(graph.triples((None, Literal("Alice"), None))) == []


def test_sparql_single_pattern(graph):
    res = list(graph.query("SELECT ?n WHERE { <http://ex.org/alice> <http://xmlns.com/foaf/0.1/name> ?n }"))
    assert res == [(Literal("Alice"),)]


def test_sparql_join(graph):
    res = list(
        graph.query(
            """SELECT ?n WHERE {
                <http://ex.org/alice> <http://xmlns.com/foaf/0.1/knows> ?x .
                ?x <http://xmlns.com/foaf/0.1/name> ?n
            }"""
        )
    )
    assert res == [(Literal("Bob", lang="en"),)]


def test_sparql_typed_literal(graph):
    res = list(graph.query("SELECT ?a WHERE { ?s <http://ex.org/age> ?a }"))
    assert len(res) == 1
    assert res[0][0].toPython() == 42


def test_literal_object_patterns(graph):
    # Bound-object lookups with language tags and datatypes go through the
    # native pattern parser and must not degrade to simple literals.
    assert list(graph.triples((None, None, Literal("Bob", lang="en")))) == [
        (BOB, FOAF_NAME, Literal("Bob", lang="en"))
    ]
    assert list(graph.triples((None, None, Literal("Bob")))) == []
    assert list(graph.triples((None, None, Literal(42)))) == [
        (BOB, URIRef("http://ex.org/age"), Literal(42))
    ]


def test_blank_node_subject(graph):
    rows = list(graph.triples((None, None, Literal("Anon"))))
    assert len(rows) == 1
    assert isinstance(rows[0][0], BNode)


def test_read_only(graph):
    with pytest.raises(NotImplementedError):
        graph.store.add((ALICE, FOAF_NAME, Literal("x")), None)
    with pytest.raises(NotImplementedError):
        graph.store.remove((None, None, None))


def test_dictionary_graph_uses_code_path(vortex_files, monkeypatch):
    store = VortexStore(str(vortex_files["dictionary"]))
    assert store._dict is not None  # code path active

    monkeypatch.setenv("VORTEX_RDF_DISABLE_CODE_PATH", "1")
    disabled = VortexStore(str(vortex_files["dictionary"]))
    assert disabled._dict is None  # string fallback forced

    # Both paths yield identical triples.
    got_codes = sorted(Graph(store=store).triples((None, None, None)))
    got_strings = sorted(Graph(store=disabled).triples((None, None, None)))
    assert got_codes == got_strings and len(got_codes) == 5


def test_in_memory_graph_equality(vortex_files):
    on_file = Graph(store=VortexStore(str(vortex_files["dictionary"])))
    in_mem = Graph(store=VortexStore(str(vortex_files["dictionary"]), in_memory=True))
    assert len(in_mem) == len(on_file) == 5
    assert sorted(in_mem.triples((None, None, None))) == sorted(
        on_file.triples((None, None, None))
    )


def test_layout_alias_and_detection(vortex_files):
    # Branch-era labels are accepted; the detected layout wins.
    store = VortexStore(str(vortex_files["dictionary"]), layout="cottas-native-ids")
    assert store.layout == "dictionary"
    store = VortexStore(str(vortex_files["default"]), layout="cottas-native-strings")
    assert store.layout == "default"


def test_from_n3_safe_dbpedia_escaped_apostrophe():
    # Language-tagged literal with a non-canonical \' escape (DBpedia quirk).
    term = VortexStore._from_n3_safe('"L\\\'agent"@fr')
    assert term == Literal("L'agent", lang="fr")


def test_no_path_store():
    store = VortexStore()
    assert len(store) == 0
    assert list(store.triples((None, None, None))) == []
