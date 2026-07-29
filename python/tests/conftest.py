import pytest

from vortex_rdflib._native import serialize_rdf

FIXTURE_NT = """\
<http://ex.org/alice> <http://xmlns.com/foaf/0.1/name> "Alice" .
<http://ex.org/alice> <http://xmlns.com/foaf/0.1/knows> <http://ex.org/bob> .
<http://ex.org/bob> <http://xmlns.com/foaf/0.1/name> "Bob"@en .
<http://ex.org/bob> <http://ex.org/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
_:b0 <http://xmlns.com/foaf/0.1/name> "Anon" .
"""

LAYOUTS = ["default", "typed-object", "dictionary"]


@pytest.fixture(scope="session")
def fixture_nt_path(tmp_path_factory):
    path = tmp_path_factory.mktemp("rdf") / "fixture.nt"
    path.write_text(FIXTURE_NT)
    return path


@pytest.fixture(scope="session")
def vortex_files(fixture_nt_path, tmp_path_factory):
    """One serialized `.vortex` file per layout, keyed by layout name."""
    out_dir = tmp_path_factory.mktemp("vortex")
    files = {}
    for layout in LAYOUTS:
        out = out_dir / f"fixture-{layout}.vortex"
        serialize_rdf(str(fixture_nt_path), str(out), layout=layout)
        files[layout] = out
    return files
