# bw25_playground

A brightway 2.5 project for automated LCA supply chain graph generation.

## Stack

- Python >= 3.11, managed with `uv`
- brightway 2.5: `bw2data>=4`, `bw2calc>=2`, `bw2io>=0.9`
- Visualization: `networkx`, `pyvis`, `plotly`, `matplotlib`

## How to run

```bash
uv run python main.py
```

This will:
1. Set up a brightway project and install `biosphere3` + LCIA methods (first run only, ~1 min)
2. Import activities and exchanges from `input/` CSVs
3. Run LCA for each activity × each impact method
4. Save graphs to `output/graphs/`

## Folder structure

```
bw25_playground/
├── main.py                  # Entry point — run this
├── pyproject.toml           # uv dependencies
├── README.md                # This file
├── uv.lock
├── tutorials/               # Reference Jupyter notebooks
├── input/
│   ├── activities.csv       # Activities to model (code, name, location, unit, database, category)
│   ├── exchanges.csv        # Technosphere + biosphere exchanges between activities
│   └── methods.csv          # LCIA impact methods to calculate
└── output/
    └── graphs/              # All generated graphs saved here
        ├── *.html           # Interactive PyVis supply chain graphs
        ├── *sankey*.html    # Plotly Sankey diagrams
        └── *bar*.png        # Matplotlib contribution bar charts
```

## CSV schemas

### activities.csv
| column | description |
|--------|-------------|
| code | unique identifier (no spaces) |
| name | human-readable name |
| location | ISO country code or region |
| unit | functional unit (kg, kWh, m3, etc.) |
| database | must match DATABASE_NAME in main.py |
| category | optional label (manufacturing, energy, etc.) |

### exchanges.csv
| column | description |
|--------|-------------|
| input_code | code of the supplying activity |
| input_database | database of the supplying activity (`my_db` or `biosphere3`) |
| output_code | code of the receiving activity |
| output_database | database of the receiving activity |
| amount | exchange amount (float) |
| type | `technosphere`, `biosphere`, or `production` |
| unit | unit of the exchange |
| comment | optional description |

### methods.csv
| column | description |
|--------|-------------|
| method_tuple | Python tuple string, e.g. `('IPCC 2021', 'climate change', 'GWP 100a')` |
| description | human-readable label |

## Cowork instructions

- Always run scripts with `uv run python <script.py>` — never `python` directly
- Input CSVs are in `input/` — edit these to change what gets modelled
- Output graphs go to `output/graphs/` — open `.html` files in a browser
- The brightway project is named `bw25_playground` and stored locally by brightway
- To re-import the database after editing CSVs, delete the database first:
  ```python
  import bw2data as bd
  bd.projects.set_current("bw25_playground")
  del bd.databases["my_db"]
  ```
- `bw_graph_tools` is used for traversal if installed; falls back to `bw2analyzer`
- Graph cutoff and max depth are set at the top of `main.py`

## Key brightway 2.5 notes

- `brightway2` meta-package is NOT used — individual packages are listed directly
- `biosphere3` elementary flows are required for any biosphere exchanges
- LCIA methods must match exactly — check available methods with `list(bd.methods)` in a Python shell
