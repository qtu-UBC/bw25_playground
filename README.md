# bw25_playground

A Brightway 2.5 playground for **model-level** LCA calculation and **supply chain graph generation** from a structured Excel foreground model.

This repo is set up to:
- parse a foreground model from `input/lci-carbon-fiber.xlsx`
- build a **foreground database** (`my_db`)
- build an **external inputs database** (`external_inputs`) for any foreground inputs that don’t match the foreground set (and/or can’t be matched to ecoinvent)
- (optionally) match technosphere inputs to **ecoinvent 3.10.1 cut-off**
- run LCIA for a single **model root** (functional unit) and generate a readable **model-level** HTML graph per impact method.

> **Important (license):** do **not** commit ecoinvent datasets to GitHub. This repo ignores `data/`, `*.spold`, and ecoinvent folders via `.gitignore`.

---

## Requirements

- Python **3.11+**
- [`uv`](https://github.com/astral-sh/uv) for env + dependency management
- A local copy of **ecoinvent 3.10.1 cut-off ecospold2** (optional, licensed)

---

## Repo layout

- `input/lci-carbon-fiber.xlsx` – foreground model (Activity blocks + Exchanges tables)
- `input/methods.csv` – LCIA methods to run (must match installed Brightway method keys exactly)
- `main.py` – end-to-end pipeline (import → match → LCIA → model-level graph)
- `output/graphs/` – generated HTML graphs (ignored by git)
- `data/` – local datasets (ecoinvent etc.) (**ignored by git**)

---

## First-time setup

```bash
uv sync
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

---

## Run

```bash
uv run python main.py
```

On the first run in a fresh Brightway project, it will ensure:
- `biosphere3` exists
- LCIA methods are installed (you should see hundreds of methods)

Then it will:
1) check/import ecoinvent (if configured and not already installed)
2) parse the Excel model into foreground + external inputs
3) write databases
4) select the **model root** and run LCIA
5) write HTML graphs into `output/graphs/`

---

## Model root (functional unit)

The “model root” is the process used as the functional unit anchor for the whole model graph + LCIA.

This repo supports selecting the root by:
- **activity name** (process name), or
- **reference product** (recommended if the process name is long)

Example: reference product `"carbon fiber, weaved"`.

If you change the Excel, keep the root selector consistent.

---

## Outputs

- `output/graphs/<root>__<method>__graph.html`

Graphs use a hierarchical left-to-right layout and show edge amounts on hover to avoid clutter.

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

---

## Notes

- ecoinvent regionalization/geocollections warnings can be ignored unless you are doing regionalized LCIA.
- If you modify the Excel schema (column names / block structure), update the parser accordingly.
