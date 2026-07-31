import pytest

from conftest import LAYOUTS

from vortex_rdf import VortexRdfStore, serialize_rdf

NAME = "<http://xmlns.com/foaf/0.1/name>"


@pytest.mark.parametrize("layout", LAYOUTS)
def test_open_len_and_layout(vortex_files, layout):
    store = VortexRdfStore(str(vortex_files[layout]))
    assert store.layout() == layout
    assert len(store) == 5


@pytest.mark.parametrize("layout", LAYOUTS)
def test_match_triples_patterns(vortex_files, layout):
    store = VortexRdfStore(str(vortex_files[layout]))

    assert len(store.match_triples()) == 5

    names = store.match_triples(p=NAME)
    assert len(names) == 3
    assert all(p == NAME for _, p, _ in names)

    bob = store.match_triples(s="<http://ex.org/bob>")
    assert sorted(o for _, _, o in bob) == [
        '"42"^^<http://www.w3.org/2001/XMLSchema#integer>',
        '"Bob"@en',
    ]

    by_object = store.match_triples(o='"Alice"')
    assert by_object == [("<http://ex.org/alice>", NAME, '"Alice"')]

    # Language-tagged and typed literal object patterns must match exactly.
    assert store.match_triples(o='"Bob"@en') == [("<http://ex.org/bob>", NAME, '"Bob"@en')]
    assert store.match_triples(o='"Bob"') == []
    typed = store.match_triples(o='"42"^^<http://www.w3.org/2001/XMLSchema#integer>')
    assert len(typed) == 1 and typed[0][1] == "<http://ex.org/age>"

    assert store.match_triples(s="<http://ex.org/nobody>") == []


@pytest.mark.parametrize("layout", LAYOUTS)
def test_match_compact_indices_are_consistent(vortex_files, layout):
    store = VortexRdfStore(str(vortex_files[layout]))
    table, rows = store.match_compact(p=NAME)
    assert len(rows) == 3
    # Every index points into the table, and the table is de-duplicated.
    assert all(i < len(table) for row in rows for i in row)
    assert len(set(table)) == len(table)
    # Reconstructed triples equal the plain form.
    reconstructed = sorted((table[s], table[p], table[o]) for s, p, o in rows)
    assert reconstructed == sorted(store.match_triples(p=NAME))


def test_code_path_matches_compact_path(vortex_files):
    store = VortexRdfStore(str(vortex_files["dictionary"]))
    dictionary = store.term_dict()
    assert dictionary is not None and len(dictionary) > 0

    for pattern in ({}, {"p": NAME}, {"s": "<http://ex.org/bob>"}, {"o": '"Bob"@en'}):
        cols = store.match_codes(**pattern)
        assert cols is not None
        views = [memoryview(c).cast("I").tolist() for c in cols[:3]]
        from_codes = sorted(
            (dictionary.decode(s), dictionary.decode(p), dictionary.decode(o))
            for s, p, o in zip(*views)
        )
        table, rows = store.match_compact(**pattern)
        from_compact = sorted((table[s], table[p], table[o]) for s, p, o in rows)
        assert from_codes == from_compact


def test_code_path_unavailable_on_other_layouts(vortex_files):
    for layout in ("default", "typed-object"):
        store = VortexRdfStore(str(vortex_files[layout]))
        assert store.term_dict() is None
        assert store.match_codes() is None


def test_u32_column_buffer_is_zero_copy_view(vortex_files):
    store = VortexRdfStore(str(vortex_files["dictionary"]))
    cols = store.match_codes()
    view = memoryview(cols[0])
    assert view.readonly
    typed = view.cast("I")
    assert len(typed) == len(cols[0]) == 5
    # Two views over the same column expose identical memory.
    assert typed.tolist() == memoryview(cols[0]).cast("I").tolist()


@pytest.mark.parametrize("layout", LAYOUTS)
def test_in_memory_open_matches_file_backed(vortex_files, layout):
    file_backed = VortexRdfStore(str(vortex_files[layout]))
    in_memory = VortexRdfStore(str(vortex_files[layout]), in_memory=True)
    assert in_memory.layout() == layout
    assert len(in_memory) == len(file_backed) == 5
    assert sorted(in_memory.match_triples()) == sorted(file_backed.match_triples())
    assert sorted(in_memory.match_triples(p=NAME)) == sorted(file_backed.match_triples(p=NAME))


def test_in_memory_dictionary_keeps_code_path(vortex_files):
    store = VortexRdfStore(str(vortex_files["dictionary"]), in_memory=True)
    dictionary = store.term_dict()
    assert dictionary is not None
    cols = store.match_codes(p=NAME)
    assert cols is not None and len(cols[0]) == 3


def test_blank_node_round_trip(vortex_files):
    store = VortexRdfStore(str(vortex_files["dictionary"]))
    rows = store.match_triples(o='"Anon"')
    assert len(rows) == 1
    assert rows[0][0].startswith("_:")


def test_bad_term_raises_value_error(vortex_files):
    store = VortexRdfStore(str(vortex_files["default"]))
    with pytest.raises(ValueError):
        store.match_triples(s="not a term")
    with pytest.raises(ValueError):
        store.match_triples(p='"literals-cannot-be-predicates"')


def test_missing_file_raises_oserror(tmp_path):
    with pytest.raises(OSError):
        VortexRdfStore(str(tmp_path / "missing.vortex"))


def test_serialize_rejects_unknown_options(fixture_nt_path, tmp_path):
    out = str(tmp_path / "out.vortex")
    with pytest.raises(ValueError):
        serialize_rdf(str(fixture_nt_path), out, layout="nope")
    with pytest.raises(ValueError):
        serialize_rdf(str(fixture_nt_path), out, builder="nope")
    with pytest.raises(ValueError):
        serialize_rdf(str(fixture_nt_path), out, format="nope")


def test_serialize_layout_aliases(fixture_nt_path, tmp_path):
    # cottas-bench branch names remain accepted.
    out = tmp_path / "alias.vortex"
    serialize_rdf(str(fixture_nt_path), str(out), layout="cottas-native-ids")
    assert VortexRdfStore(str(out)).layout() == "dictionary"


def test_dictionary_file_is_self_contained(fixture_nt_path, tmp_path):
    out = tmp_path / "dict.vortex"
    serialize_rdf(str(fixture_nt_path), str(out), layout="dictionary")
    # The embedded form is one file: no companion appears beside it.
    assert list(tmp_path.iterdir()) == [out]
    store = VortexRdfStore(str(out))
    assert store.layout() == "dictionary"
    assert len(store) == 5
    assert len(store.match_triples(p=NAME)) == 3
