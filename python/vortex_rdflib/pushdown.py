"""SPARQL BGP pushdown for :class:`~vortex_rdflib.store.VortexStore`.

rdflib's default ``evalBGP`` is a nested-loop join that calls
``Store.triples()`` once per candidate binding, and every one of those calls
pays the native match floor. This module registers a ``CUSTOM_EVALS`` hook
that evaluates a whole basic graph pattern against a VortexStore in one
pass instead:

- every triple pattern is matched natively exactly once, returning u32
  term-code columns (``match_codes``);
- the join runs in code space — hash joins over ``int`` tuples, no term
  strings, no rdflib term construction for intermediate results;
- terms are decoded only for the final solutions, each distinct code once,
  through the store's decode cache.

The hook applies only when the active graph's store is a VortexStore with
the code path available (Dictionary layout, resident dictionary); anything
else — other stores, other algebra nodes, RDF-star patterns — raises
``NotImplementedError``, which makes rdflib fall through to its default
evaluator. Behavior is therefore identical to the default path, only faster.

Registration happens automatically when the first ``VortexStore`` is
constructed; set ``VORTEX_RDF_DISABLE_PUSHDOWN=1`` to keep the default
evaluator (e.g. for A/B benchmarking).
"""

import os

from rdflib.plugins.sparql import CUSTOM_EVALS
from rdflib.term import BNode, Literal, URIRef, Variable

_EVAL_KEY = "vortex_rdflib_bgp"

# Query bnodes act as variables, exactly as rdflib's evalBGP treats them.
_VAR_LIKE = (Variable, BNode)


def register_sparql_pushdown():
    """Install the BGP hook into rdflib's CUSTOM_EVALS (idempotent)."""
    if os.environ.get("VORTEX_RDF_DISABLE_PUSHDOWN") == "1":
        return
    CUSTOM_EVALS.setdefault(_EVAL_KEY, _eval_part)


def unregister_sparql_pushdown():
    CUSTOM_EVALS.pop(_EVAL_KEY, None)


def _eval_part(ctx, part):
    if part.name != "BGP":
        raise NotImplementedError

    from .store import VortexStore

    store = getattr(getattr(ctx, "graph", None), "store", None)
    if not isinstance(store, VortexStore) or store._dict is None:
        raise NotImplementedError

    # Solve eagerly: the CUSTOM_EVALS dispatcher only catches
    # NotImplementedError at call time, so nothing may fail lazily after a
    # generator is handed back.
    schema, rows = _solve_bgp(ctx, store, part.triples)
    return _yield_solutions(ctx, store, schema, rows)


def _solve_bgp(ctx, store, triples):
    """Evaluate the BGP in code space: ``(schema, rows)`` where ``schema`` is
    a tuple of variable-like terms and each row a tuple of u32 codes."""
    if not triples:
        return (), [()]

    tp_results = []
    for s, p, o in triples:
        for term in (s, p, o):
            # RDF-star quoted triples (or anything else exotic) in a pattern
            # position: not supported here, use the default evaluator.
            if not isinstance(term, (*_VAR_LIKE, URIRef, Literal)):
                raise NotImplementedError
        tp_results.append(_solve_pattern(ctx, store, s, p, o))

    # Start from the smallest pattern result, then greedily prefer patterns
    # sharing a variable with the schema so far (avoids cross products).
    tp_results.sort(key=lambda t: len(t[1]))
    schema, rows = tp_results[0]
    remaining = tp_results[1:]
    while remaining and rows:
        pick = next(
            (i for i, (sch, _) in enumerate(remaining) if any(v in schema for v in sch)),
            0,
        )
        schema, rows = _join(schema, rows, *remaining.pop(pick))
    return schema, rows


def _solve_pattern(ctx, store, s, p, o):
    """One native match for one triple pattern, as ``(schema, rows)``."""
    rs, rp, ro = ctx[s], ctx[p], ctx[o]

    # rdflib joins can propagate a literal into subject or predicate
    # position; that pattern is unsatisfiable, not an error.
    if rs is not None and not isinstance(rs, (URIRef, BNode)):
        return (), []
    if rp is not None and not isinstance(rp, URIRef):
        return (), []

    # Positions left variable, with same-variable repeats turned into
    # post-filters (e.g. `?x :p ?x` needs s == o).
    positions = {}
    eq_checks = []
    for idx, (term, value) in enumerate(zip((s, p, o), (rs, rp, ro))):
        if value is None:
            if term in positions:
                eq_checks.append((positions[term], idx))
            else:
                positions[term] = idx

    to_n3 = store._node_to_n3
    cols = store._store().match_codes(to_n3(rs), to_n3(rp), to_n3(ro))
    if cols is None:
        raise NotImplementedError
    views = [memoryview(c).cast("I").tolist() for c in cols[:3]]

    if eq_checks:
        keep = [
            i
            for i in range(len(views[0]))
            if all(views[a][i] == views[b][i] for a, b in eq_checks)
        ]
        views = [[v[i] for i in keep] for v in views]

    schema = tuple(positions.keys())
    if not schema:
        # Fully ground pattern: no bindings, one (empty) row per match, so a
        # non-matching ground pattern still eliminates all solutions.
        return (), [()] * len(views[0])
    return schema, list(zip(*(views[positions[v]] for v in schema)))


def _join(schema_a, rows_a, schema_b, rows_b):
    """Hash join of two code-space relations on their shared variables."""
    shared = [v for v in schema_b if v in schema_a]
    keep_b = [i for i, v in enumerate(schema_b) if v not in schema_a]
    schema = schema_a + tuple(schema_b[i] for i in keep_b)

    if not shared:
        return schema, [ra + rb for ra in rows_a for rb in rows_b]

    ia = [schema_a.index(v) for v in shared]
    ib = [schema_b.index(v) for v in shared]
    table = {}
    for rb in rows_b:
        key = tuple(rb[i] for i in ib)
        table.setdefault(key, []).append(tuple(rb[i] for i in keep_b))
    out = []
    for ra in rows_a:
        tails = table.get(tuple(ra[i] for i in ia))
        if tails:
            out.extend(ra + tail for tail in tails)
    return schema, out


def _yield_solutions(ctx, store, schema, rows):
    decode = store._decode_term
    for row in rows:
        c = ctx.push()
        for term, code in zip(schema, row):
            c[term] = decode(code)
        yield c.solution()
