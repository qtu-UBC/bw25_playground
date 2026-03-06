# bw25_playground

A Brightway 2.5 playground for **model-level** LCA calculation and **supply chain graph generation** from a structured Excel foreground model.

This repo is set up to:
- parse foreground models from all `*.xlsx` files in `input/activity tables/`
- build a **foreground database** (`my_db`)
- build an **external inputs database** (`external_inputs`) for any foreground inputs that don’t match the foreground set (and/or can’t be matched to ecoinvent)
- (optionally) match technosphere inputs to **ecoinvent 3.10.1 cut-off**
- run LCIA for one or more **model roots** (functional units) per file and generate readable **model-level** HTML graphs per impact method.

> **Important (license):** do **not** commit ecoinvent datasets to GitHub. This repo ignores `data/`, `*.spold`, and ecoinvent folders via `.gitignore`.

---

## Requirements

- Python **3.11+**
- [`uv`](https://github.com/astral-sh/uv) for env + dependency management
- A local copy of **ecoinvent 3.10.1 cut-off ecospold2** (optional, licensed)

---

## Repo layout

- `input/activity tables/*.xlsx` – foreground models (Activity blocks + Exchanges tables)
- `input/methods.csv` – LCIA methods to run (must match installed Brightway method keys exactly)
- `main.py` – end-to-end pipeline (import → match → LCIA → model-level graph)
- `output/graphs/` – generated HTML graphs (ignored by git)
- `output/logs/biosphere_skips.log` – skipped unknown biosphere flows
- `output/logs/errors.log` – runtime/write errors with tracebacks
- `data/` – local datasets (ecoinvent etc.) (**ignored by git**)

---

## First-time setup

```bash
uv sync
uv run python scripts/bootstrap_bw.py
```

### Configure ecoinvent (optional, recommended)

Create a `.env` file (repo root) and set the ecospold directory path:

```bash
cat > .env <<'EOF'
ECOINVENT_ECOSPOlD_DIR=/Users/qtu-ubc/Documents/bw25_playground/data/ecoinvent 3.10.1_cutoff_ecoSpold02/datasets
EOF
```

- The path must point to a directory containing many `*.spold` files (recursively is OK).
- `.env` is ignored by git.

If ecoinvent is not configured, the script will still run, but unmatched inputs will be stubbed into `external_inputs`.

### Brightway bootstrap

`scripts/bootstrap_bw.py` is the reproducible project bootstrap for this repo. It:
- ensures `biosphere3` exists
- installs default LCIA methods (with compatibility handling for the current pinned stack)
- runs an LCIA sanity check (non-zero characterization linkage)

Run it whenever you create a fresh Brightway project or suspect broken method linkage:

```bash
uv run python scripts/bootstrap_bw.py
```

---

## Run

```bash
uv run python main.py
```

On the first run in a project that has been bootstrapped, it will:

Then it will:
1) check/import ecoinvent (if configured and not already installed)
2) discover all `.xlsx` files in `input/activity tables/` (Excel temp files like `~$...xlsx` are ignored)
3) parse each file into foreground + external inputs
4) write databases
5) select model roots per file:
   - include `ROOT_ACTIVITY_NAME` if present
   - include inferred roots (activities not consumed by other foreground technosphere exchanges)
6) run LCIA and write HTML graphs into `output/graphs/`


---

## Outputs

- `output/graphs/<source_file>__<root_or_multi_root>__<method>__graph.html`
- `output/logs/biosphere_skips.log`
- `output/logs/errors.log`

Graphs use a hierarchical left-to-right layout and show edge amounts on hover to avoid clutter.

### Graph interactions

Each generated HTML graph includes client-side controls:
- Click a node to highlight its upstream/downstream path.
- Click empty space to reset.
- `Highlight Shared Nodes` toggles nodes shared across all final product nodes (purple highlight).
- Edge threshold filter:
  - slider: `0.1%` to `100%`
  - numeric input box for typing an exact `%`
  - hides edges below the selected percentage of the graph's max edge amount.
- In shared mode, a scrollable table lists shared node names.

---

## Git hygiene (important)

### Do not commit ecoinvent

This repo’s `.gitignore` already includes:

- `data/`
- `**/*.spold`
- `**/ecoinvent*/`

If you accidentally pushed ecoinvent once, you must **rewrite history** (ignore rules don’t remove already-pushed blobs). Recommended:

```bash
brew install git-filter-repo
git filter-repo --path "data/ecoinvent 3.10.1_cutoff_ecoSpold02" --invert-paths
git push --force --all
git push --force --tags
```

---

## Troubleshooting

### `ValueError: Excel file format cannot be determined`
This is usually an Excel lock/temp file (e.g. `~$lci-trucks.xlsx`).  
The script now skips these automatically.

### `ValueError: Unknown biosphere flow name: '...'`
Unknown biosphere flows are skipped (not fatal) and logged to:
- `output/logs/biosphere_skips.log`

### `AttributeError: 'float' object has no attribute 'lower'`
This can happen when Excel text fields are `NaN`/float.  
The script normalizes text fields before writing databases. If write-time errors still occur, details are logged to:
- `output/logs/errors.log`

### “No valid methods found”
Your `input/methods.csv` entries must match installed Brightway method keys **exactly**.
List installed methods:

```bash
uv run python - <<'PY'
import bw2data as bd
bd.projects.set_current("bw25_playground")
print(len(bd.methods))
for m in list(bd.methods)[:50]:
    print(m)
PY
```

### PyVis template / rendering errors
Install Jinja2 and ensure HTML export uses `write_html` (not notebook mode):

```bash
uv pip install jinja2
```

### LCIA scores are all zero
Run project bootstrap again:

```bash
uv run python scripts/bootstrap_bw.py
```

Then rerun:

```bash
uv run python main.py
```

---

## Notes

- ecoinvent regionalization/geocollections warnings can be ignored unless you are doing regionalized LCIA.
- If you modify the Excel schema (column names / block structure), update the parser accordingly.
