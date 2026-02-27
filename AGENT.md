# DTPR — Columnar Refactor Reference

> **No backward compatibility.** The old `particle_types / expr / filter / sorter`
> keys and the `enrich()` pass are being replaced entirely.  
> Current test baseline: **16 passed, 7 pre-existing CLI failures** (unchanged).

---

## ⚙️ Verified Environment Facts (DO NOT skip this section)

```
Python         : 3.12.3  (venv at /home/destrada/test/DTPatternRecognition/env/)
ROOT           : 6.32.16 (system site-packages, always available)
awkward        : 2.9.0
coffea         : 2025.12.0
uproot         : 5.7.1
```

### A. Awkward v2 behavior API — CONFIRMED

```python
behavior = {}

class MyRecord(ak.Record): ...
class MyArray(ak.Array): ...

behavior["MyThing"] = MyRecord       # single-record dispatch key
behavior["*MyThing"] = MyArray       # collection dispatch key

arr = ak.zip({...}, with_name="MyThing", behavior=behavior)
# arr[0]  → MyRecord instance ✅
# arr     → plain ak.Array (NOT MyArray) when sliced from a Record field
```

> **Known**: When a `Particle` collection is accessed as `event["digis"]` (slicing from an
> `EventRecord`), the result is a plain `ak.Array`, not `ParticleArray`. The `ParticleRecord`
> dispatch on individual elements (`event["digis"][0]`) works correctly. Fixing the array-level
> dispatch for `ParticleArray` requires either (a) wrapping with
> `ak.Array(layout, behavior=behavior)` after the slice, or (b) defining a `__getitem__`
> on `EventRecord` that rewraps. Decide this during Step 3 — it is a cosmetic issue only,
> all field access still works.

### B. coffea `zip_forms` — CONFIRMED signature

Located in `coffea.nanoevents.schemas.base` (same module as `BaseSchema`):

```python
from coffea.nanoevents.schemas.base import zip_forms

zip_forms(
    forms: dict,          # {attr_name: form_dict}  — all must be the same class (ListOffsetArray or NumpyArray)
    name: str,            # internal name used for form key, e.g. "digis"
    record_name: str,     # sets __record__ parameter, e.g. "Particle"
    offsets=None,         # optional — leave None for jagged branches
    bypass=False,         # optional — leave False
)
```

> The AGENT.md original said "second argument sets `__record__`" — **WRONG**. The
> `record_name` is the **third positional / keyword** argument. The second argument is the
> collection name (used only for the form key).

### C. `NanoEventsFactory.from_root` — CONFIRMED signature (coffea 2025.12.0)

```python
NanoEventsFactory.from_root(
    file,                          # str, dict, or list of {file: treepath}
    *,
    mode='virtual',                # 'eager', 'virtual', or 'dask'
    treepath=uproot._util.unset,
    entry_start=None,
    entry_stop=None,
    schemaclass=NanoAODSchema,     # pass PatternSchema.with_config(config) here
    metadata=None,
    uproot_options={},
    iteritems_options={},          # dict passed to tree.iteritems — use {"filter_name": allowed_set} for branch allow-listing
    access_log=None,
    use_ak_forth=True,
    known_base_form=None,
    ...
)
```

Pipeline inside `from_root`:
1. Opens file(s) with uproot, calls `tree.iteritems(**iteritems_options)` to extract branch forms.
2. Calls `schemaclass(base_form)` — no extra args.
3. Returns a `NanoEventsFactory` instance; call `.events()` to get the actual `ak.Array`.

> **Branch allow-listing**: pass `iteritems_options={"filter_name": allowed_branches}` where
> `allowed_branches` is a Python `set` of branch-name strings. This is the uproot `filter_name`
> parameter — accepts a set, list, callable, or regex. Using a set of strings is simplest.

### D. Schema instantiation — CONFIRMED

coffea calls `schemaclass(base_form)` with exactly one positional arg.
Config injection is therefore done via a bound subclass returned by `PatternSchema.with_config(config)`:

```python
schema_cls = PatternSchema.with_config(my_config)
events = NanoEventsFactory.from_root(files, schemaclass=schema_cls, ...).events()
```

### E. Branch allow-listing — decision made

`event_*` branches are all readable (confirmed by inspection of the ROOT file). Include **all**
`event_*` branches implicitly; there is no need for an explicit `extra_branches` YAML key unless
the user adds non-`event_*` scalar branches they need. For now:

```python
allowed = set()
# 1. particle branch: values from config
for ptype, pinfo in config.particle_types.items():
    for attr_name, attr_info in pinfo.get("attributes", {}).items():
        if isinstance(attr_info, dict) and attr_info.get("branch"):
            allowed.add(attr_info["branch"])
# 2. all event_ branches found in the file
allowed |= {b for b in tree_keys if b.startswith("event_")}
# 3. optional extra_branches from config
allowed |= set(getattr(config, "extra_branches", []) or [])
```

### F. ROOT file branch count

The test ROOT file (`DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root`,
tree `dtNtupleProducer/DTTREE`) has **323 total branches**. Most are not in the config.
Many (nested-vector `seg_phiHits_*`, `mu_matches_*`, etc.) cannot be deserialized by uproot
at all — coffea emits "Skipping X as it cannot be represented as an Awkward array" warnings.
The allow-list approach in Section E above eliminates all those warnings automatically.

---

## 📁 Current Codebase State

| File | State |
|------|-------|
| `dtpr/base/particle.py` | ✅ Keep — `ParticleRecord(ak.Record)`, `ParticleArray(ak.Array)`, `behavior` |
| `dtpr/base/event.py` | ✅ Keep — `EventRecord(ak.Record)`, merged `behavior` |
| `dtpr/base/schema.py` | 🔄 **Rewrite** — new flat Schema format (see below) |
| `dtpr/base/ntuple.py` | 🔄 **Simplify** — remove `enrich()` call, wire in pipeline executor |
| `dtpr/base/enrichment.py` | ❌ **Delete** — replaced by pipeline |
| `dtpr/base/event_list.py` | ❌ Already deleted — `ntuple.events` is a raw `dask_awkward.Array` |
| `dtpr/base/old_particle.py` | ❌ **Delete** — no compat needed |
| `dtpr/base/old_event.py` | ❌ **Delete** — no compat needed |
| `dtpr/base/pipeline.py` | ✅ **Done** — topological executor; simple sequential `events→events` model (see below) |
| `dtpr/utils/preprocessors.py` | 🔄 **Update** — rewrite to explicit per-step functions |
| `dtpr/utils/selectors.py` | 🔄 **Update** — rewrite to explicit per-step functions |
| `dtpr/utils/yamls/run_config.yaml` | 🔄 **Rewrite** — new `Schema:` + `pipeline:` keys |
| `dtpr/base/__init__.py` | 🔄 **Update** — export `EventRecord`, `ParticleRecord`; remove old aliases |
| `tests/test_ntuple.py` | 🔄 **Update** — use new YAML structure |

---

## 🏗️ New Design

### 1. YAML config structure

The config file has three top-level keys:

```
ntuple_tree_name  — ROOT TTree path inside the file
Schema            — what to read and how to structure it
pipeline          — what to compute / filter, in what order
```

---

#### `Schema`

Controls what branches are read and how they are structured into an Awkward record.

**Three forms:**

```yaml
# Form 1 — string: use a named coffea schema class as-is
Schema: "NanoAODSchema"

# Form 2 — absent or null: read ALL branches using coffea BaseSchema (default)
# Schema:     ← omit the key entirely, or leave value null

# Form 3 — mapping: custom PatternSchema with the rules below
Schema:
  ...
```

**Mapping rules (Form 3):**

| Entry type | Example | Meaning |
|---|---|---|
| Top-level key, `string` value | `run: "event_runNumber"` | event-level scalar branch |
| Top-level key, `numeric` value | `version: 1` | event-level constant field (no branch) |
| Top-level key, `dict` value | `digis: {...}` | particle collection |
| Within collection, `string` value | `wh: "digi_wheel"` | attribute → ROOT branch name |
| Within collection, `numeric` value | `dummy: -999` | attribute → constant (no branch) |

**Full example:**

```yaml
ntuple_tree_name: "/dtNtupleProducer/DTTREE"

Schema:
  # event-level scalars
  run:  "event_runNumber"
  lumi: "event_lumiBlock"

  # particle collections
  digis:
    wh:   "digi_wheel"
    sec:  "digi_sector"
    st:   "digi_station"
    sl:   "digi_superLayer"
    l:    "digi_layer"
    wire: "digi_wire"
    time: "digi_time"
    BX:   "digi_BX"

  tps:
    quality: "ph2TpgPhiEmuAm_quality"
    BX:      "ph2TpgPhiEmuAm_BX"
    phi:     "ph2TpgPhiEmuAm_phi"
    phiB:    "ph2TpgPhiEmuAm_phiB"

  segments:
    wh:  "seg_wheel"
    sec: "seg_sector"
    st:  "seg_station"
    phi: "seg_posLoc_x"

  genmuons:
    pt:    "genPart_pt"
    eta:   "genPart_eta"
    phi:   "genPart_phi"
    pdgId: "genPart_pdgId"
```

---

#### `pipeline`

Describes every transformation applied to the loaded event array. Replaces the old
`ntuple_selectors`, `ntuple_preprocessors`, and the entire `enrich()` pass.

**Step body keys** (the YAML mapping key is the step name — no `name:` sub-key):

| Key | Required | Description |
|---|---|---|
| `type` | ✅ | `selector` or `preprocessor` |
| `target` | selector only, optional | collection name (e.g. `tps`) for particle-level filtering; absent = event-level |
| `expr` | one of `expr`/`src` | inline Python string evaluated with `{"events": events, "ak": ak}` in scope |
| `src` | one of `expr`/`src` | dotted import path to a callable — called as `fn(events)` |
| `depends_on` | optional | list of step names that must run before this step; default `[]` |

Exactly one of `expr` or `src` must be present. Validate at config-load time and raise
`ValueError` otherwise.

**Step semantics:**

```python
# type: selector, no target  →  event-level filter
events = events[step_result(events)]        # step_result returns bool[nevents]

# type: selector, target: "tps"  →  particle-level filter
mask = step_result(events)                  # bool[nevents × nparticles] (variable-length)
events = ak.with_field(events, events["tps"][mask], "tps")

# type: preprocessor  →  arbitrary events → events transform
# The callable owns all ak.with_field / ak.zip calls; it receives the full
# event array and must return the modified event array.
events = step_fn(events)
```

**Execution ordering rule — selectors first within each dependency level:**

After topological sort, steps are grouped by depth level. Within each level,
**all selectors execute before all preprocessors**. This drops bad/unwanted events
before any expensive computation runs.

```
level-0 selectors → level-0 preprocessors → level-1 selectors → level-1 preprocessors → …
```

**Full example:**

```yaml
pipeline:
  # level 0 — selector: runs before any level-0 preprocessors
  select-has-genmuons:
    type: selector
    expr: "ak.num(events['genmuons']) > 0"
    depends_on: []

  # level 0 — preprocessors: run after all level-0 selectors
  center-tp-BX:
    type: preprocessor
    src: dtpr.utils.preprocessors.center_tp_BX
    depends_on: []

  sort-digis-by-BX:
    type: preprocessor
    src: dtpr.utils.preprocessors.sort_digis_by_BX
    depends_on: []

  # level 0 — particle-level selector
  filter-genmuons-pdgid:
    type: selector
    target: genmuons
    src: dtpr.utils.selectors.filter_genmuons_pdgid
    depends_on: []

  # level 1 — selector: depends on center-tp-BX
  filter-tp-quality:
    type: selector
    target: tps
    expr: "events['tps']['quality'] >= 0"
    depends_on: [center-tp-BX]

  # level 2 — preprocessor
  sort-tps-by-BX:
    type: preprocessor
    src: dtpr.utils.preprocessors.sort_tps_by_BX
    depends_on: [filter-tp-quality]
```

---

### 2. Updated `PatternSchema` (`dtpr/base/schema.py`)

`PatternSchema.__init__` still receives only `base_form` (coffea requirement — see Verified
Fact D). Config is injected via `with_config(schema_map)` where `schema_map` is the value
of the `Schema` key in the YAML (a dict, already parsed — not the whole config).

**Allow-list extraction** (called from `ntuple.py` before building the factory):

```python
def _branches_from_schema(schema_map: dict) -> set[str]:
    """All ROOT branch names referenced in the Schema mapping."""
    branches = set()
    for key, val in schema_map.items():
        if isinstance(val, str):
            branches.add(val)           # event-level branch
        elif isinstance(val, dict):
            for attr, v in val.items():
                if isinstance(v, str):
                    branches.add(v)     # particle-level branch
    return branches
```

**Form building** — for each collection (dict value), call `zip_forms`:

```python
zip_forms(
    {attr: base_form_contents[branch] for attr, branch in col.items() if isinstance(branch, str)},
    name=col_name,          # e.g. "digis"
    record_name="Particle", # all collections use "Particle" so behavior dispatches
)
```

Event-level scalar branches (top-level string values) are added to the base form directly
without `zip_forms`.

**Constant fields** (numeric values — no corresponding ROOT branch): injected after
`factory.events()` returns the dask array, before the pipeline runs:

```python
def _inject_constants(events: ak.Array, schema_map: dict) -> ak.Array:
    for key, val in schema_map.items():
        if isinstance(val, (int, float)):
            events = ak.with_field(events, val, key)          # event-level constant
        elif isinstance(val, dict):
            col = events[key]
            for attr, v in val.items():
                if isinstance(v, (int, float)):
                    col = ak.with_field(col, v, attr)          # particle-level constant
            events = ak.with_field(events, col, key)
    return events
```

---

### 3. Pipeline executor (`dtpr/base/pipeline.py`)

```python
import awkward as ak
from ..utils.functions import get_callable_from_src

def topological_sort(steps: dict[str, dict]) -> list[list[tuple[str, dict]]]:
    """
    Group steps by dependency depth.
    Returns list of levels; each level is a list of (name, step_body) tuples.
    Raises ValueError on unknown depends_on references or cycles.
    """
    # ... (memoised depth computation + cycle detection via sentinel -1)

def _make_fn(name: str, step: dict):
    """Return the callable described by a step's expr or src."""

def execute_pipeline(events, steps: dict[str, dict]):
    """
    Execute pipeline steps in dependency order.
    Within each level: selectors first (reduce data), then preprocessors (transform).
    Preprocessors are pure events → events; they own their ak.with_field calls.
    """
    for level_group in topological_sort(steps):
        selectors     = [(n, s) for n, s in level_group if s["type"] == "selector"]
        preprocessors = [(n, s) for n, s in level_group if s["type"] == "preprocessor"]
        for name, step in selectors:
            mask = _make_fn(name, step)(events)
            if target := step.get("target"):
                events = ak.with_field(events, events[target][mask], target)
            else:
                events = events[mask]
        for name, step in preprocessors:
            events = _make_fn(name, step)(events)
    return events
```

**Key design decisions:**
- `topological_sort` returns `(name, step_body)` tuples — no `name` key injected into step dicts.
- `_make_fn(name, step)` takes `name` explicitly — no dict mutation.
- Preprocessors are fully general `events → events`; the callable owns all field writes.
- No patch mode / conflict detection — users are trusted to write non-conflicting steps.
- All real parallelism comes from dask at the file/partition level.

---

### 4. `ntuple.py` loading flow

```python
def _load_events(self) -> ak.Array:
    # 1. Resolve input → {file_path: treepath} dict
    file_map = _preprocess_input(self._input, self._maxfiles, self._treepath)

    # 2. Determine schema class and allow-list
    schema_section = self._config.get("Schema")
    uproot_opts: dict = {}

    if schema_section is None:
        schema_cls = BaseSchema                             # read all branches
    elif isinstance(schema_section, str):
        schema_cls = _resolve_coffea_schema(schema_section)
    else:
        allowed = _branches_from_schema(schema_section)    # from schema.py
        uproot_opts = {"filter_name": allowed}
        schema_cls = PatternSchema.with_config(schema_section)

    # 3. Build dask graph (no disk reads beyond file metadata)
    events = NanoEventsFactory.from_root(
        file_map,
        schemaclass=schema_cls,
        mode="dask",
        uproot_options=uproot_opts,
    ).events()

    # 4. Inject constant fields declared in Schema (numeric values)
    if isinstance(schema_section, dict):
        events = _inject_constants(events, schema_section)

    # 5. Execute user pipeline
    events = execute_pipeline(events, self._config.get("pipeline", {}))

    return events
```

**Public API (unchanged):**

```python
ntuple = NTuple("/path/to/dir/")
ntuple.events                               # dask_awkward.Array — fully lazy
ntuple.events.eager_compute_divisions()     # read entry counts (fast)
len(ntuple.events)                          # after eager_compute_divisions()
ntuple.events[0].compute()                  # single EventRecord
ntuple.events["digis"]["BX"].compute()      # columnar, all files in parallel
```

---

## 🔧 Implementation Steps

### A — Rewrite `run_config.yaml`

Replace all `particle_types`, `ntuple_selectors`, `ntuple_preprocessors` with the new
`Schema:` and `pipeline:` keys. This is the single source of truth that drives everything.

### B — Rewrite `dtpr/base/schema.py`

- Remove `particle_types` parsing logic entirely.
- `with_config(schema_map)` now binds the Schema mapping dict (not the whole config).
- `_branches_from_schema(schema_map)` as module-level helper (also used by `ntuple.py`).
- `_build_collections(schema_map, base_form_contents)` builds one `zip_forms` call per
  dict-valued entry in `schema_map`.
- Event-level scalar branches (string-valued top-level entries) go directly into the
  base form without `zip_forms`.

### C — `dtpr/base/pipeline.py` — **DONE**

Fully implemented. See Design §3.

- `topological_sort(steps: dict[str, dict])` — groups by dependency depth, returns `list[list[tuple[str, dict]]]`.
- `_make_fn(name, step)` — compiles `expr` or resolves `src` callable via `get_callable_from_src`.
- `execute_pipeline(events, steps)` — sequential: selectors first, then `events→events` preprocessors.
- `tests/test_pipeline.py` — covers: topological sort, cycle detection, unknown dep, event-level selector, particle-level selector, preprocessor transform, unknown type error, empty steps.

### D — Simplify `dtpr/base/ntuple.py`

- Remove `enrich()` import and call.
- Remove separate `_selectors` / `_preprocessors` loop.
- Add `_inject_constants()` call after `factory.events()`.
- Import and call `execute_pipeline()`.
- Keep `_preprocess_input()` unchanged.

### E — Update `dtpr/utils/preprocessors.py` and `selectors.py`

All callables must follow the signature `fn(events: ak.Array) -> ak.Array`. Selectors
return a boolean array; preprocessors return the modified events array. Remove any
wrappers from the old expr/src/sorter/filter mechanism.

### F — Update `dtpr/base/__init__.py`

```python
from .event    import EventRecord as Event      # alias for downstream user code
from .particle import ParticleRecord as Particle
from .ntuple   import NTuple
```

### G — Delete dead files

```
dtpr/base/enrichment.py
dtpr/base/old_event.py
dtpr/base/old_particle.py
```

### H — Update tests

- `tests/test_ntuple.py` — update fixture YAML to `Schema:` + `pipeline:` keys.
- `tests/test_pipeline.py` — new file (see Step C above).
- `tests/test_config_include.py` — update fixture YAMLs that reference old keys.

### Validation

```bash
env/bin/python3 -m pytest tests/ -v
# target: ≥16 passed, ≤7 pre-existing CLI failures
```

---

## 📎 Implementation Notes

- **`ak.with_field` is immutable** — always reassign:
  `col = ak.with_field(col, new_val, "name")`.

- **Particle-level selector**: the `target` key tells the executor to call
  `events[target][mask]`. The callable still receives the full `events` and must return
  the per-particle boolean mask (variable-length jagged array matching the collection shape).

- **`expr` namespace**: only `events` and `ak` are in scope. If a step needs other
  imports, use `src` instead of `expr`.

- **`depends_on` defaults to `[]`**: a step without a `depends_on` key reads from the
  raw loaded array — maximum parallelism case in the dask graph.

- **Dask laziness**: `execute_pipeline` builds dask graph nodes but never triggers
  computation. Independent steps (same base array, no shared ancestor) appear as sibling
  branches in the graph. `.compute()` is only called by the user.

- **`Schema` string form** (`Schema: "NanoAODSchema"`): resolve via
  `getattr(coffea.nanoevents.schemas, name)`. Raise `ValueError` if not found.

- **`Schema` absent**: `BaseSchema` reads all 323 branches including many that uproot
  cannot deserialize — coffea will emit "Skipping X" warnings. Fine for exploration;
  use a mapping Schema in production to suppress warnings and reduce read overhead.

---

## 🔮 Future Features / Refactors

### Patch-mode parallel preprocessors

**Idea:** Allow preprocessors within the same dependency level to run on the same
`base_events` snapshot (in parallel as sibling dask branches) instead of chaining
sequentially.  Each would return `{field_name: value}` patches that are validated
for conflicts and merged via `ak.with_field` after all branches complete.

**Why it was deferred:** The patch dict contract conflicts with the natural way to
write nested field modifications (e.g. `events["digis"]["BX"]`), which require
returning a full events array.  Most real-world pipeline steps are logically
sequential transforms, and the dominant parallelism comes from dask at the
file/partition level — intra-level step parallelism provides minimal additional gain.

**What would be needed to implement:**
- Validate all-or-nothing: if any step in a level returns a non-dict, fall back to
  sequential mode (legacy path).
- Conflict detection: ensure no two steps in the same level write the same field.
- Length validation: for eager arrays, check patch lengths match `len(base_events)`;
  skip for dask-like objects (lengths unknown at graph-construction time).
- Optional `parallel=True` flag on `execute_pipeline` to opt in.

**Reference:** A full working implementation existed in commits before the
simplification to the current `events→events` model.  See git history for
`execute_pipeline` with `patch_mode`, `merged_patch`, and `_validate_patch`.

### Adaptive vector methods on `ParticleRecord`

**Idea:** Make `ParticleRecord` optionally inherit `vector` math methods
(e.g. `delta_r()`, `boost()`, `to_xyz()`) depending on what fields the
underlying awkward record actually has — detected at runtime, not hardcoded.

**Motivation:** Keeps the framework fully generic (any NTuple, any field names)
while still giving physics-convenient methods when the data supports them.
No need to subclass per collection type.

**Sketch:**

```
YAML declares a collection with phi/eta/pt/mass fields
    ↓
PatternSchema sets __record__ = "Particle" as usual
    ↓
ParticleRecord.__init_subclass__ or a factory detects available fields
    ↓
Dynamically mixes in the right vector base:
  - pt, eta, phi, mass  →  vector.PtEtaPhiMLorentzVector
  - pt, eta, phi, energy → vector.PtEtaPhiELorentzVector
  - x, y, z, t           → vector.LorentzVector
  - x, y, z              → vector.ThreeVector (if vector supports it)
  - none of the above    → plain ParticleRecord (current behavior)
```

**YAML opt-in option (alternative):** User explicitly declares the vector type
in the Schema YAML to avoid runtime field introspection:

```yaml
Schema:
  collections:
    segments:
      branches: [...]
      vector: PtEtaPhiM   # optional — enables vector methods
```

**Viability:** High. `vector` already integrates with awkward behavior dicts.
The dynamic mixin is non-trivial but feasible via `type()` factory or
`ak.mixin_class`. The YAML opt-in is simpler and more explicit.

**Constraint to preserve:** `behavior["Particle"]` must remain a single key.
Named subtypes (`Muon`, `Jet`) could be supported by letting YAML declare
`record_name: Muon` per collection, which would register `behavior["Muon"]`
dynamically at schema-build time — still fully declarative, no hardcoded classes.
