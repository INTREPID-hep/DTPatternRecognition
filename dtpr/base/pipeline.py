"""
Pipeline executor for the columnar DT Pattern Recognition framework.

Topological ordering
--------------------
Steps are grouped by dependency depth (level 0 = no dependencies, level 1 =
depends on at least one level-0 step, etc.).  Within each level, **selectors
run before preprocessors** to discard unwanted events/particles as early as
possible before any expensive computation.

Step semantics
--------------
* ``type: selector``, no ``target`` — event-level filter:
  ``events = events[fn(events)]``   where ``fn`` returns ``bool[nevents]``

* ``type: selector``, with ``target: "tps"`` — particle-level filter:
  ``mask = fn(events)``  (bool per particle, variable-length)
  ``events = ak.with_field(events, events["tps"][mask], "tps")``

* ``type: preprocessor`` — arbitrary transform:
    ``fn(events)``  where ``fn`` **mutates** the event array directly using
    ``events["field"] = value`` (leverages ``ak.Array.__setitem__`` /
    ``dak.Array.__setitem__``).  No return value is used.  For nested
    collections use ``events["col"] = ak.with_field(events["col"], …, "key")``.

Step definition keys
--------------------
The ``pipeline:`` key in YAML is a **mapping** (dict), where each key is the
unique step name.  No ``name:`` sub-key is needed.

* ``type``       — ``"selector"`` or ``"preprocessor"``
* ``target``     — (selector only) collection name for particle-level filter
* ``expr``       — inline Python evaluated with ``{"events": events, "ak": ak}``
* ``src``        — dotted import path to ``fn(events) -> None``  (mutates in-place)
* ``depends_on`` — list of step names that must run first (default ``[]``)

Exactly one of ``expr`` / ``src`` must be present.
"""

from __future__ import annotations

import awkward as ak
from ..utils.functions import get_callable_from_src


# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------

def topological_sort(steps: dict[str, dict]) -> list[list[dict]]:
    """Group pipeline steps by dependency depth.

    Parameters
    ----------
    steps : dict[str, dict]
        Mapping of step-name → step-body as parsed from the YAML ``pipeline:``
        key.  Each value must not contain a redundant ``name`` key — the dict
        key *is* the name.

    Returns a list of *levels*.  Each level is a list of ``(name, step)``
    tuples whose maximum dependency depth equals the level index.  Pairs
    within a level are independent of each other.

    Raises
    ------
    ValueError
        If a step references an unknown ``depends_on`` name, or if a cycle
        is detected in the dependency graph.
    """
    if not steps:
        return []

    deps_map: dict[str, list[str]] = {}
    for name, step in steps.items():
        deps = step.get("depends_on", [])
        if not isinstance(deps, list):
            raise ValueError(
                f"Step '{name}': depends_on must be a list of step names."
            )
        unknown = set(deps) - steps.keys()
        if unknown:
            raise ValueError(
                f"Step '{name}' depends_on unknown step(s): {unknown}"
            )
        deps_map[name] = deps

    level_cache: dict[str, int] = {}

    def _level(name: str) -> int:
        if name in level_cache:
            return level_cache[name]
        # Detect cycles via a sentinel
        level_cache[name] = -1  # "in progress"
        parents = deps_map[name]
        if any(level_cache.get(p) == -1 for p in parents):
            raise ValueError(f"Cycle detected in pipeline involving step '{name}'.")
        result = 0 if not parents else 1 + max(_level(p) for p in parents)
        level_cache[name] = result
        return result

    for name in steps:
        _level(name)

    max_level = max(level_cache.values(), default=0)
    return [
        [(name, steps[name]) for name in steps if level_cache[name] == lvl]
        for lvl in range(max_level + 1)
    ]


# ---------------------------------------------------------------------------
# Step function factory
# ---------------------------------------------------------------------------

def _make_fn(name: str, step: dict, kind: str = "eval"):
    """Return the callable described by a step's ``expr`` or ``src`` key.

    Parameters
    ----------
    kind : {'eval', 'exec'}
        ``'eval'`` — compiles ``expr`` as an expression; return value is used
        (selectors: returns a boolean mask).
        ``'exec'`` — compiles ``expr`` as a statement so that assignments such
        as ``events["field"] = value`` are valid; return value is discarded
        (preprocessors).
    """
    has_expr = "expr" in step
    has_src = "src" in step

    if has_expr and has_src:
        raise ValueError(
            f"Step '{name}': specify exactly one of 'expr' or 'src', not both."
        )
    if not has_expr and not has_src:
        raise ValueError(
            f"Step '{name}': must have exactly one of 'expr' or 'src'."
        )

    if has_expr:
        if kind == "exec":
            code = compile(step["expr"], f"<pipeline:{name}>", "exec")
            def _fn(events, _c=code):
                exec(_c, {"events": events, "ak": ak})
            return _fn
        code = compile(step["expr"], f"<pipeline:{name}>", "eval")
        return lambda events, _c=code: eval(_c, {"events": events, "ak": ak})

    return get_callable_from_src(step["src"])


# ---------------------------------------------------------------------------
# Pipeline executor
# ---------------------------------------------------------------------------

def execute_pipeline(events, steps: dict[str, dict]):
    """Execute pipeline steps in dependency order.

    Within each dependency level, **selectors run before preprocessors** to
    reduce the dataset size before any expensive computation.

    Selectors return a boolean mask; the executor applies it internally.
    Preprocessors **mutate** the event array in-place via ``__setitem__``
    (``events["field"] = value``); no return value is captured.

    All real parallelism is provided by dask at the file/partition level.
    Step ordering within a level is determined by the ``depends_on`` graph
    (topological sort); within the same level, selectors always precede
    preprocessors.

    Parameters
    ----------
    events : ak.Array or dask_awkward.Array
        The loaded event array.  If lazy (dask-awkward), no computation is
        triggered — this function only builds graph nodes.
    steps : dict[str, dict]
        Pipeline step definitions as parsed from the YAML ``pipeline:`` key.
        Each key is the step name; each value is the step body (``type``,
        ``expr``/``src``, optional ``depends_on``, optional ``target``).

    Returns
    -------
    ak.Array or dask_awkward.Array
        The transformed event array (still lazy if input was lazy).
    """
    for level_group in topological_sort(steps):
        # Fail fast: validate step types before executing anything in this level.
        unknown = [step["type"] for _, step in level_group
                   if step["type"] not in {"selector", "preprocessor"}]
        if unknown:
            raise ValueError(
                f"Unknown step type(s): {unknown}. Must be 'selector' or 'preprocessor'."
            )

        selectors     = [(n, s) for n, s in level_group if s["type"] == "selector"]
        preprocessors = [(n, s) for n, s in level_group if s["type"] == "preprocessor"]

        # Selectors first: drop unwanted events/particles before any expensive work.
        for name, step in selectors:
            fn = _make_fn(name, step)
            target = step.get("target")
            mask = fn(events)
            if target:
                # particle-level: mask within the named collection
                events = ak.with_field(events, events[target][mask], target)
            else:
                # event-level: drop entire events
                events = events[mask]

        # Preprocessors: mutate events in-place via __setitem__, applied sequentially.
        for name, step in preprocessors:
            _make_fn(name, step, kind="exec")(events)

    return events


# ---------------------------------------------------------------------------
# Quick smoke-test
# Run with:  python -m dtpr.base.pipeline
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import dask_awkward as dak

    events = ak.Array([
        {"num": 1, "x": 10, "digis": [{"wh": -1}, {"wh": 2}]},
        {"num": 2, "x": 20, "digis": [{"wh": 1}, {"wh": -2}]},
        {"num": 3, "x": 30, "digis": [{"wh": 3}]},
    ])

    steps = {
        "keep_even_events": {
            "type": "selector",
            "expr": "events['num'] % 2 == 0",
        },
        "add_x2": {
            "type": "preprocessor",
            "expr": "events['x2'] = events['x'] * 2",
            "depends_on": ["keep_even_events"],
        },
        "add_xp1": {
            "type": "preprocessor",
            "expr": "events['xp1'] = events['x'] + 1",
            "depends_on": ["keep_even_events"],
        },
        "keep_positive_wh": {
            "type": "selector",
            "target": "digis",
            "expr": "events['digis']['wh'] > 0",
            "depends_on": ["add_x2", "add_xp1"],
        },
    }

    print("=" * 60)
    print("pipeline.py smoke-test")
    print("=" * 60)

    levels = topological_sort(steps)
    print("Topological levels:", [[name for name, _ in grp] for grp in levels])
    print("Input num:", events["num"].to_list())
    print("Input digis.wh:", events["digis"]["wh"].to_list())

    out = execute_pipeline(events, steps)

    print("Output num:", out["num"].to_list())
    print("Output x2:", out["x2"].to_list())
    print("Output xp1:", out["xp1"].to_list())
    print("Output digis.wh:", out["digis"]["wh"].to_list())

    assert out["num"].to_list() == [2]
    assert out["x2"].to_list() == [40]
    assert out["xp1"].to_list() == [21]
    assert out["digis"]["wh"].to_list() == [[1]]

    print("\n--- Dask graph demo ---")
    devents = dak.from_awkward(events, npartitions=2)
    dout = execute_pipeline(devents, steps)

    graph = dout.__dask_graph__()
    print("Graph type:", type(graph).__name__)
    if hasattr(graph, "layers"):
        print("Graph layers:", list(graph.layers.keys()))
    if hasattr(graph, "dependencies"):
        print("\nLayer dependencies (trimmed):")
        for lname in graph.layers:
            if any(tok in lname for tok in ("multiply", "add-", "with-field", "from-awkward", "x-")):
                deps = sorted(graph.dependencies.get(lname, []))
                print(f"  - {lname} <- {deps}")
    print("Approx task count:", len(graph))

    try:
        dout.visualize(filename="pipeline_dask_graph.svg")
        print("Graph visualization written to: pipeline_dask_graph.svg")
    except Exception as exc:
        print(f"Graph visualization skipped ({type(exc).__name__}: {exc})")

    dout_comp = dout.compute()
    assert dout_comp["num"].to_list() == [2]
    assert dout_comp["x2"].to_list() == [40]
    assert dout_comp["xp1"].to_list() == [21]
    assert dout_comp["digis"]["wh"].to_list() == [[1]]

    print("All assertions passed ✓")
