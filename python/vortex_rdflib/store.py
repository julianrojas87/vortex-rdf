import os
from pathlib import Path
from typing import Optional

from rdflib.store import NO_STORE, VALID_STORE, Store
from rdflib.term import BNode, Literal, Node, URIRef
from rdflib.util import from_n3

from ._native import VortexRdfStore
from .pushdown import register_sparql_pushdown

# The cottas-bench branch's layout names are accepted as aliases so its
# benchmark scripts keep working. The layout is only a label here: VortexRdfStore
# auto-detects the actual layout from the file.
_LAYOUT_ALIASES = {
    "default": "default",
    "cottas-native-strings": "default",
    "typed-object": "typed-object",
    "typed_object": "typed-object",
    "dictionary": "dictionary",
    "cottas-native-ids": "dictionary",
    "cottas-native": "dictionary",
}


def _term_debug(t):
    if t is None:
        return "None"
    try:
        return t.n3()
    except Exception:
        return repr(t)


class VortexStore(Store):
    """Read-only rdflib Store over a `.vortex` file.

    SPARQL evaluation is rdflib's engine; this store serves triple patterns.
    Use as ``Graph(store=VortexStore("data.vortex"))``.
    """

    context_aware = False
    formula_aware = False
    transaction_aware = False
    graph_aware = False

    def __init__(
        self,
        configuration=None,
        identifier=None,
        path: Optional[str] = None,
        layout: Optional[str] = None,
        backend: str = "native",
        max_resident_terms: Optional[int] = None,
        in_memory: Optional[bool] = None,
        **kwargs,
    ):
        # IMPORTANT:
        # RDFLib Store.__init__ may call self.open(configuration).
        # Therefore all attributes used by open() must exist BEFORE super().__init__.
        if path is None:
            path = configuration

        self.path = str(Path(path)) if path is not None else None
        self.layout = _LAYOUT_ALIASES.get(layout, layout) if layout else None
        self.backend = backend
        self.max_resident_terms = max_resident_terms
        # File-backed lazy open by default; in-memory drops the ~1 ms per-call
        # file-scan floor (decisive for rdflib joins) at the cost of loading
        # the store up front. Env override for benchmark sweeps.
        if in_memory is None:
            in_memory = os.environ.get("VORTEX_RDF_IN_MEMORY") == "1"
        self.in_memory = in_memory
        self._native: Optional[VortexRdfStore] = None
        self._dict = None
        self._decode_cache: dict = {}
        self._use_codes = os.environ.get("VORTEX_RDF_DISABLE_CODE_PATH") != "1"

        # Whole-BGP pushdown into code space (no-op for non-Vortex graphs;
        # VORTEX_RDF_DISABLE_PUSHDOWN=1 keeps rdflib's default evaluator).
        register_sparql_pushdown()

        # Do not pass configuration here, otherwise RDFLib calls open()
        # before our initialization logic is fully under control.
        super().__init__(configuration=None, identifier=identifier)

        # Explicitly open after initialization.
        if self.path is not None:
            self.open(self.path)

    def open(self, configuration, create=False):
        """RDFLib Store API: configuration is the Vortex file path."""
        if self.backend != "native":
            raise ValueError(
                f"Unsupported Vortex backend: {self.backend!r} (only 'native' is available)"
            )

        if configuration is not None:
            self.path = str(Path(configuration))

        if self.path is None:
            return NO_STORE

        self._native = VortexRdfStore(
            self.path,
            max_resident_terms=self.max_resident_terms,
            in_memory=self.in_memory,
        )
        # Record what the file actually is, whatever label the caller passed.
        self.layout = self._native.layout()
        # Dictionary-layout stores with a resident dictionary serve matches as
        # u32 code columns; each distinct code is decoded to an rdflib term
        # once and cached. None on other layouts -> string fallback path.
        self._dict = self._native.term_dict() if self._use_codes else None
        self._decode_cache = {}
        return VALID_STORE

    def close(self, commit_pending_transaction=False):
        self._native = None
        self._dict = None
        self._decode_cache = {}

    def _store(self) -> VortexRdfStore:
        if self._native is None:
            if self.path is None:
                raise ValueError("VortexStore has no path; pass one or call open()")
            self.open(self.path)
        return self._native

    def triples(self, triple_pattern, context=None):
        """RDFLib Store API: yields ((s, p, o), context) for a pattern."""
        if self.path is None:
            return

        s, p, o = triple_pattern

        # RDFLib can propagate an object binding into subject or predicate
        # position during joins. That pattern is unsatisfiable, not an error.
        if s is not None and not isinstance(s, (URIRef, BNode)):
            return
        if p is not None and not isinstance(p, URIRef):
            return

        s_n3 = self._node_to_n3(s)
        p_n3 = self._node_to_n3(p)
        o_n3 = self._node_to_n3(o)

        if os.environ.get("VORTEX_RDF_TRACE_TRIPLES") == "1":
            print(
                "[VortexStore.triples:start] "
                f"layout={self.layout} "
                f"s={_term_debug(s)} p={_term_debug(p)} o={_term_debug(o)} "
                f"s_n3={s_n3} p_n3={p_n3} o_n3={o_n3}",
                flush=True,
            )

        store = self._store()

        if self._dict is not None:
            cols = store.match_codes(s_n3, p_n3, o_n3)
            if cols is not None:
                # Zero-copy u32 views over the Rust column buffers; decode
                # each distinct code to an rdflib term once, cached for the
                # store's lifetime (codes are stable in a read-only store).
                s_codes = memoryview(cols[0]).cast("I").tolist()
                p_codes = memoryview(cols[1]).cast("I").tolist()
                o_codes = memoryview(cols[2]).cast("I").tolist()
                decode = self._decode_term
                for row in zip(s_codes, p_codes, o_codes):
                    yield (decode(row[0]), decode(row[1]), decode(row[2])), None
                return

        lexical_terms, indexed_rows = store.match_compact(s_n3, p_n3, o_n3)
        parsed_terms = [self._from_n3_safe(value) for value in lexical_terms]
        term_count = len(parsed_terms)
        for s_idx, p_idx, o_idx in indexed_rows:
            if s_idx >= term_count or p_idx >= term_count or o_idx >= term_count:
                raise ValueError(
                    "Compact native result has an invalid term index: "
                    f"{(s_idx, p_idx, o_idx)!r}, terms={term_count}"
                )
            yield (
                parsed_terms[s_idx],
                parsed_terms[p_idx],
                parsed_terms[o_idx],
            ), None

    def _decode_term(self, code: int) -> Node:
        node = self._decode_cache.get(code)
        if node is None:
            raw = self._dict.decode(code)
            if raw is None:
                raise ValueError(f"term code {code} is not in the store dictionary")
            node = self._from_n3_safe(raw)
            self._decode_cache[code] = node
        return node

    def __len__(self, context=None):
        if self.path is None:
            return 0
        return len(self._store())

    def add(self, triple, context=None, quoted=False):
        raise NotImplementedError("VortexStore is read-only")

    def addN(self, quads):
        raise NotImplementedError("VortexStore is read-only")

    def remove(self, triple_pattern, context=None):
        raise NotImplementedError("VortexStore is read-only")

    def bind(self, prefix, namespace, override=True):
        return None

    def namespace(self, prefix):
        return None

    def namespaces(self):
        return iter(())

    def prefix(self, namespace):
        return None

    @staticmethod
    def _node_to_n3(node: Optional[Node]) -> Optional[str]:
        if node is None:
            return None
        return node.n3()

    @staticmethod
    def _from_n3_safe(value: str) -> Node:
        # URIRefs and blank nodes have unambiguous standalone N3 forms.  Build
        # them directly rather than routing every dictionary term through
        # RDFLib's generic from_n3 parser.  Besides avoiding parser overhead,
        # this accepts valid absolute IRIs that from_n3 rejects in some RDFLib
        # versions/configurations when no namespace manager is supplied.
        if len(value) >= 2 and value[0] == "<" and value[-1] == ">":
            return URIRef(value[1:-1])
        if value.startswith("_:"):
            return BNode(value[2:])

        try:
            term = from_n3(value)
        except Exception as error:
            # DBpedia contains a small number of language-tagged literals with
            # an escaped apostrophe (\') even though the literal itself is
            # double quoted.  RDFLib rejects that non-canonical escape.  The
            # fallback is deliberately restricted to simple, language-tagged,
            # double-quoted literals; typed and multiline literals still fail
            # loudly through the generic error below.
            split = value.rfind('"@')
            if value.startswith('"') and split > 0:
                lexical = value[1:split]
                language = value[split + 2:]
                if language and all(
                    character.isalnum() or character == "-"
                    for character in language
                ):
                    lexical = lexical.replace("\\'", "'")
                    try:
                        return Literal(from_n3(f'"{lexical}"').value, lang=language)
                    except Exception:
                        pass
            raise ValueError(
                f"Could not parse returned RDF term as N3: {value!r}"
            ) from error

        if term is None:
            raise ValueError(
                f"RDFLib returned no RDF term for N3 value: {value!r}"
            )
        return term
