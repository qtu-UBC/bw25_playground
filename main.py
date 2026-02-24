
"""
bw25_playground - main.py (xlsx + model-level graph)
====================================================

This script:
  1) Ensures Brightway project exists (assumes you ran your bootstrap for biosphere3 + LCIA methods)
  2) Imports a foreground system from an Excel sheet (input/lci-carbon-fiber.xlsx, sheet "Carbon fiber")
     into database "my_db" (foreground activities defined by "Activity" blocks in the sheet).
  3) Uses a TWO-DB approach for technosphere inputs that are not defined as foreground activities:
        - If ecoinvent-3.10.1-cutoff is installed, try to match inputs to ecoinvent.
        - Otherwise (or if no match), create a stub activity in "external_inputs".
  4) Runs LCIA for a single model functional unit (ROOT_ACTIVITY_NAME, amount ROOT_ACTIVITY_AMOUNT)
     for each method listed in input/methods.csv (rows are method tuples: level_1, level_2, level_3).
  5) Generates ONE model-level supply-chain graph per method (HTML) under output/graphs/.

Run:
    uv run python main.py

Notes:
  - Ecoinvent is licensed. This script can only "install" it if you already have the ecospold files locally.
    Set environment variable ECOINVENT_ECOSPOlD_DIR to the directory containing the .spold files (ecospold2).
"""

from __future__ import annotations

import os
import re
import glob
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Set

import pandas as pd
import networkx as nx
from pyvis.network import Network

import bw2data as bd
import bw2calc as bc
import bw2io as bi


# ---------------------------
# User-configurable settings
# ---------------------------

PROJECT_NAME = "bw25_playground"

# Foreground & external dbs
FOREGROUND_DB = "my_db"
EXTERNAL_DB = "external_inputs"

# Excel input
XLSX_PATH = Path("input") / "lci-carbon-fiber.xlsx"
XLSX_SHEET = "Carbon fiber"

# Model root (functional unit)
ROOT_ACTIVITY_NAME = "carbon fiber production, weaved, at factory"
ROOT_ACTIVITY_AMOUNT = 1.0

# Methods list (CSV with columns: level_1, level_2, level_3)
METHODS_CSV = Path("input") / "methods.csv"

# Output
OUTPUT_DIR = Path("output")
GRAPHS_DIR = OUTPUT_DIR / "graphs"

# Graph settings
GRAPH_MAX_NODES = 250          # hard cap for readability
GRAPH_MAX_DEPTH = 6            # traverse technosphere inputs up to this depth
INCLUDE_BACKGROUND_NODES = True  # include matched ecoinvent nodes as leaves (no further expansion)


# ---------------------------
# Helpers
# ---------------------------

def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "item"


def ensure_dirs() -> None:
    GRAPHS_DIR.mkdir(parents=True, exist_ok=True)


def set_project() -> None:
    bd.projects.set_current(PROJECT_NAME)


def force_delete_db(db_name: str) -> None:
    """Hard delete a BW database, including registry ghost entries."""
    set_project()
    if db_name in bd.databases:
        del bd.databases[db_name]
    try:
        bd.Database(db_name).delete(warn=False)
    except Exception:
        pass


# ---------------------------
# Ecoinvent presence / install
# ---------------------------

ECOINVENT_DB = "ecoinvent-3.10.1-cutoff"


def ecoinvent_installed() -> bool:
    set_project()
    return ECOINVENT_DB in bd.databases


def try_install_ecoinvent() -> bool:
    """
    Try to import ecoinvent if it is not installed, using local ecospold files.

    Requires environment variable:
      ECOINVENT_ECOSPOlD_DIR = path to directory containing many *.spold files
    """
    set_project()
    if ecoinvent_installed():
        return True

    spold_dir = os.environ.get("ECOINVENT_ECOSPOlD_DIR") or os.environ.get("ECOINVENT_ECOSPOld_DIR")
    if not spold_dir:
        print(f"[ecoinvent] '{ECOINVENT_DB}' not installed.")
        print("[ecoinvent] To auto-import, set env var ECOINVENT_ECOSPOlD_DIR to your local ecospold2 directory.")
        print("[ecoinvent] Continuing without ecoinvent; unmatched technosphere inputs will be stubbed in 'external_inputs'.")
        return False

    spold_dir = os.path.expanduser(spold_dir)
    spold_files = glob.glob(os.path.join(spold_dir, "**", "*.spold"), recursive=True)
    if not spold_files:
        print(f"[ecoinvent] No .spold files found under: {spold_dir}")
        print("[ecoinvent] Continuing without ecoinvent; unmatched technosphere inputs will be stubbed in 'external_inputs'.")
        return False

    print(f"[ecoinvent] Importing ecoinvent from {spold_dir} ... (this can take a while)")
    importer = bi.SingleOutputEcospold2Importer(spold_dir, ECOINVENT_DB)
    importer.apply_strategies()
    importer.statistics()

    # Option 2: Try to resolve biosphere3 ↔ ecoinvent incompatibilities by adding missing flows
    # into the biosphere database, then re-link.
    try:
        if hasattr(importer, "add_unlinked_flows_to_biosphere_database"):
            importer.add_unlinked_flows_to_biosphere_database()
            importer.apply_strategies()
            importer.statistics()
    except Exception as e:
        print(f"[ecoinvent] ⚠️  Could not add unlinked biosphere flows automatically: {e!r}")

    # If there are still unlinked exchanges, drop them so we can write the DB (fallback).
    try:
        if hasattr(importer, "drop_unlinked"):
            importer.drop_unlinked(i_am_sure=True)
    except Exception as e:
        print(f"[ecoinvent] ⚠️  Could not drop unlinked exchanges automatically: {e!r}")

    # Write database
    importer.write_database()
    print(f"[ecoinvent] ✅ Imported '{ECOINVENT_DB}' with {len(bd.Database(ECOINVENT_DB))} activities")
    return True


# ---------------------------
# Excel parsing (Activity blocks)
# ---------------------------

def parse_lci_xlsx(xlsx_path: Path, sheet_name: str) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns:
      activities: {activity_name: meta}
      exchanges:  list of exchanges dict:
        {
          "output_name": str,
          "input_name": str,
          "amount": float,
          "unit": str,
          "type": str,               # technosphere / biosphere / production
          "database": str,           # as in sheet
          "location": str,
          "reference_product": str,
        }
    """
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Excel file not found: {xlsx_path}")

    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None)

    # Identify activity blocks by "Activity" marker in col 0
    markers = df.index[df[0].astype(str).str.strip().eq("Activity")].tolist()
    if not markers:
        raise ValueError("No 'Activity' blocks found. Expected 'Activity' in column A.")

    activities: Dict[str, Dict[str, Any]] = {}
    exchanges: List[Dict[str, Any]] = []

    def find_value(block: pd.DataFrame, label: str) -> Optional[Any]:
        rows = block.index[block[0].astype(str).str.strip().eq(label)].tolist()
        if not rows:
            return None
        r = rows[0]
        return block.loc[r, 1]

    for i, start in enumerate(markers):
        end = markers[i + 1] if i + 1 < len(markers) else len(df)
        block = df.iloc[start:end].copy()

        act_name = str(block.iloc[0, 1]).strip()
        if not act_name or act_name.lower() == "nan":
            continue

        meta = {
            "name": act_name,
            "comment": find_value(block, "comment"),
            "source": find_value(block, "source"),
            "location": str(find_value(block, "location") or "GLO").strip(),
            "production_amount": find_value(block, "production amount"),
            "reference_product": str(find_value(block, "reference product") or act_name).strip(),
            "unit": str(find_value(block, "unit") or "unit").strip(),
        }
        activities[act_name] = meta

        # Exchanges table
        ex_rows = block.index[block[0].astype(str).str.strip().eq("Exchanges")].tolist()
        if not ex_rows:
            continue
        ex_start = ex_rows[0] + 1

        sub = df.iloc[ex_start:end].copy().dropna(how="all")
        # header row begins where first col == "name"
        hdr_rows = sub.index[sub[0].astype(str).str.strip().eq("name")].tolist()
        if not hdr_rows:
            continue
        hdr_row = hdr_rows[0]
        header = sub.loc[hdr_row].tolist()

        data = sub.loc[hdr_row + 1:].copy().dropna(how="all")
        data.columns = header[: len(data.columns)]

        # Normalize column names to strings
        data.columns = [str(c).strip() for c in data.columns]

        for _, r in data.iterrows():
            in_name = str(r.get("name", "")).strip()
            if not in_name or in_name.lower() == "nan":
                continue

            ex_type = str(r.get("type", "")).strip()
            amount = r.get("amount", 0.0)
            try:
                amount = float(amount)
            except Exception:
                amount = 0.0

            exchanges.append(
                {
                    "output_name": act_name,
                    "input_name": in_name,
                    "amount": amount,
                    "unit": str(r.get("unit", "") or "").strip(),
                    "type": ex_type,
                    "database": str(r.get("database", "") or "").strip(),
                    "location": str(r.get("location", "") or "").strip(),
                    "reference_product": str(r.get("reference product", "") or "").strip(),
                }
            )

    return activities, exchanges


# ---------------------------
# Biosphere mapping
# ---------------------------

# Explicit aliases (your choice)
BIOSPHERE_ALIASES = {
    "Carbon dioxide": ("biosphere3", "16eeda8a-1ea2-408e-ab37-2648495058dd"),  # Carbon dioxide, fossil (air, lower strat+upper tropo)
    "CO2": ("biosphere3", "16eeda8a-1ea2-408e-ab37-2648495058dd"),
    "Carbon dioxide, fossil": ("biosphere3", "16eeda8a-1ea2-408e-ab37-2648495058dd"),
}


def resolve_biosphere_flow(flow_name: str) -> Tuple[str, str]:
    """Resolve a biosphere3 flow by alias, exact match, then contains match (preferring air + fossil)."""
    flow_name = (flow_name or "").strip()
    if flow_name in BIOSPHERE_ALIASES:
        return BIOSPHERE_ALIASES[flow_name]

    bio = bd.Database("biosphere3")

    # exact name
    exact = [f for f in bio if f.get("name") == flow_name]
    if exact:
        return exact[0].key

    # contains fallback
    name_l = flow_name.lower()
    matches = [f for f in bio if name_l in f["name"].lower()]
    if not matches:
        raise ValueError(f"Unknown biosphere flow name: {flow_name!r}")

    def score(f):
        cats = "/".join(f.get("categories") or ()).lower()
        n = f["name"].lower()
        return (
            ("air" in cats),
            ("fossil" in n),
            ("lower stratosphere" in cats or "upper troposphere" in cats),
        )

    best = sorted(matches, key=score, reverse=True)[0]
    return best.key


# ---------------------------
# Import to Brightway (two DBs)
# ---------------------------

def build_foreground_and_external(
    activities: Dict[str, Dict[str, Any]],
    exchanges: List[Dict[str, Any]],
    bg_index: Optional[Dict[Tuple[str, str, str], Tuple[str, str]]] = None,
) -> Tuple[Dict[Tuple[str, str], Dict[str, Any]], Dict[Tuple[str, str], Dict[str, Any]]]:
    """
    Build db_data for:
      - FOREGROUND_DB: Activity blocks in sheet
      - EXTERNAL_DB: stub activities for unmatched technosphere inputs
    """
    fg_codes = {slugify(name): name for name in activities.keys()}

    fg_data: Dict[Tuple[str, str], Dict[str, Any]] = {}
    ext_data: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # Create foreground datasets
    for name, meta in activities.items():
        code = slugify(name)
        key = (FOREGROUND_DB, code)
        fg_data[key] = {
            "name": name,
            "code": code,
            "database": FOREGROUND_DB,
            "location": meta.get("location") or "GLO",
            "unit": meta.get("unit") or "unit",
            "reference product": meta.get("reference_product") or name,
            "comment": meta.get("comment") or "",
            "exchanges": [
                {
                    "input": key,
                    "output": key,
                    "amount": 1.0,
                    "type": "production",
                    "unit": meta.get("unit") or "unit",
                }
            ],
        }

    def ensure_external(name: str, unit: str = "unit") -> Tuple[str, str]:
        code = slugify(name)
        key = (EXTERNAL_DB, code)
        if key in ext_data:
            return key
        ext_data[key] = {
            "name": name,
            "code": code,
            "database": EXTERNAL_DB,
            "location": "GLO",
            "unit": unit or "unit",
            "reference product": name,
            "exchanges": [
                {"input": key, "output": key, "amount": 1.0, "type": "production", "unit": unit or "unit"}
            ],
        }
        return key

    # Add exchanges to foreground datasets
    for ex in exchanges:
        out_key = (FOREGROUND_DB, slugify(ex["output_name"]))
        ex_type = str(ex.get("type") or "").strip().lower()
        in_name = str(ex.get("input_name") or "").strip()
        amount = float(ex.get("amount") or 0.0)
        unit = str(ex.get("unit") or "").strip() or "unit"

        # Skip empty / zero
        if not in_name or amount == 0.0:
            continue

        if ex_type == "production":
            continue

        # Determine input key
        if ex_type == "biosphere":
            input_key = resolve_biosphere_flow(in_name)

        elif ex_type == "technosphere":
            in_code = slugify(in_name)

            # If it is one of our foreground activities, link internally
            if in_code in fg_codes:
                input_key = (FOREGROUND_DB, in_code)
            else:
                # Try background match first if ecoinvent index is available
                input_key = None
                if bg_index is not None:
                    # build a matcher key: (name, ref prod, location) - use sheet-provided if present else blanks
                    refp = (ex.get("reference_product") or "").strip()
                    loc = (ex.get("location") or "").strip()
                    k = (in_name, refp, loc)
                    if k in bg_index:
                        input_key = bg_index[k]
                    else:
                        # fallback ignoring ref product / location
                        k2 = (in_name, "", "")
                        if k2 in bg_index:
                            input_key = bg_index[k2]

                # If no match, create external stub
                if input_key is None:
                    input_key = ensure_external(in_name, unit=unit)

        else:
            # default: treat as technosphere
            input_key = ensure_external(in_name, unit=unit)

        # Append exchange
        if out_key not in fg_data:
            # should not happen, but avoid crash
            fg_data[out_key] = {
                "name": ex["output_name"],
                "code": out_key[1],
                "database": FOREGROUND_DB,
                "location": "GLO",
                "unit": "unit",
                "reference product": ex["output_name"],
                "exchanges": [{"input": out_key, "output": out_key, "amount": 1.0, "type": "production", "unit": "unit"}],
            }

        fg_data[out_key]["exchanges"].append(
            {
                "input": input_key,
                "output": out_key,
                "amount": amount,
                "type": "biosphere" if ex_type == "biosphere" else "technosphere",
                "unit": unit,
                "comment": "",
            }
        )

    return fg_data, ext_data


def build_ecoinvent_index() -> Dict[Tuple[str, str, str], Tuple[str, str]]:
    """
    Build a simple lookup index for ecoinvent activities by (name, reference product, location).
    Also add a relaxed key (name, "", "").
    """
    idx: Dict[Tuple[str, str, str], Tuple[str, str]] = {}
    db = bd.Database(ECOINVENT_DB)

    for act in db:
        name = act.get("name", "")
        refp = act.get("reference product", "")
        loc = act.get("location", "")
        key = (name, refp, loc)
        idx[key] = act.key
        idx[(name, "", "")] = act.key  # relaxed fallback

    return idx


def write_databases(fg_data, ext_data) -> None:
    """Write/rewrite BW databases.

    IMPORTANT: Write EXTERNAL_DB first so that technosphere exchanges in FOREGROUND_DB
    that reference external stub processes are valid at write time.
    """
    set_project()

    # Remove any existing versions to avoid "ghost" metadata entries
    if FOREGROUND_DB in bd.databases:
        force_delete_db(FOREGROUND_DB)
    if EXTERNAL_DB in bd.databases:
        force_delete_db(EXTERNAL_DB)

    # Write external inputs FIRST
    if ext_data:
        bd.Database(EXTERNAL_DB).write(ext_data)
    else:
        # Ensure DB exists (optional) – skip creating empty DB
        pass

    # Then write foreground
    bd.Database(FOREGROUND_DB).write(fg_data)


# ---------------------------
# Methods
# ---------------------------

def load_methods_from_csv(path: Path) -> List[Tuple[str, str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"methods.csv not found: {path}")
    df = pd.read_csv(path)
    cols = [c for c in df.columns]
    # Expect level_1, level_2, level_3 (but allow first 3 columns)
    if {"level_1", "level_2", "level_3"}.issubset(df.columns):
        methods = list(zip(df["level_1"], df["level_2"], df["level_3"]))
    else:
        methods = [tuple(df.iloc[i, :3].astype(str).tolist()) for i in range(len(df))]
    # Filter to those installed
    available = set(bd.methods)
    valid = [m for m in methods if m in available]
    missing = [m for m in methods if m not in available]
    for m in missing:
        print(f"[warning] Method not found, skipping: {m}")
    if not valid:
        raise RuntimeError("No valid methods found. Ensure LCIA methods are installed in this Brightway project.")
    return valid


# ---------------------------
# Model-level graph + LCIA
# ---------------------------

def get_activity_by_name(db_name: str, name: str) -> bd.backends.peewee.Activity:
    db = bd.Database(db_name)
    matches = [a for a in db if a.get("name") == name]
    if matches:
        return matches[0]
    # fallback by slugified code
    code = slugify(name)
    matches = [a for a in db if a.get("code") == code]
    if matches:
        return matches[0]
    raise ValueError(f"Root activity not found in {db_name}: {name!r}")


def build_model_graph_from_foreground(root_key: Tuple[str, str], depth: int = GRAPH_MAX_DEPTH) -> nx.DiGraph:
    """
    Build a model-level dependency graph by traversing technosphere exchanges.
    Expands foreground and external nodes; includes background nodes as leaves.
    """
    G = nx.DiGraph()
    visited: Set[Tuple[str, str]] = set()

    def add_node(key: Tuple[str, str], label: str, kind: str) -> None:
        if key not in G:
            G.add_node(key, label=label, kind=kind)

    def expand(key: Tuple[str, str], d: int) -> None:
        if key in visited or d > depth:
            return
        visited.add(key)

        db_name, code = key
        try:
            act = bd.get_activity(key)
        except Exception:
            return

        # only expand foreground/external
        if db_name not in {FOREGROUND_DB, EXTERNAL_DB}:
            return

        for exc in act.technosphere():
            inp = exc.input.key  # tuple
            amt = float(exc["amount"])
            u = exc.get("unit", "")
            add_node(inp, getattr(exc.input, "get")("name"), kind=("background" if inp[0] == ECOINVENT_DB else ("external" if inp[0] == EXTERNAL_DB else "foreground")))
            G.add_edge(inp, key, label=f"{amt:g} {u}".strip(), amount=amt)

            # expand if foreground/external, else stop
            if inp[0] in {FOREGROUND_DB, EXTERNAL_DB}:
                expand(inp, d + 1)
            elif INCLUDE_BACKGROUND_NODES:
                # do not expand background to avoid huge graphs
                pass

    # Root virtual node
    root_virtual = ("__model__", "root")
    G.add_node(root_virtual, label="MODEL_ROOT", kind="root")
    add_node(root_key, label=bd.get_activity(root_key).get("name"), kind="foreground")
    G.add_edge(root_key, root_virtual, label=f"{ROOT_ACTIVITY_AMOUNT:g}", amount=ROOT_ACTIVITY_AMOUNT)

    expand(root_key, 0)

    # limit size for readability
    if G.number_of_nodes() > GRAPH_MAX_NODES:
        # Keep BFS order nodes up to cap
        keep = {root_virtual}
        q = [root_virtual]
        while q and len(keep) < GRAPH_MAX_NODES:
            cur = q.pop(0)
            for _, v in G.out_edges(cur):
                if v not in keep:
                    keep.add(v)
                    q.append(v)
                if len(keep) >= GRAPH_MAX_NODES:
                    break
        G = G.subgraph(keep).copy()

    return G


def export_graph_html(G: nx.DiGraph, out_path: Path, title: str) -> None:
    """Export a readable HTML graph.

    Uses a hierarchical left-to-right layout (better for supply chains),
    shortens long labels on-canvas, and moves details to hover tooltips.
    """
    net = Network(height="1200px", width="100%", directed=True)
    net.heading = title

    # Hierarchical layout reads much better than force-directed for dense supply-chain graphs
    net.set_options("""
    {
      "layout": {
        "hierarchical": {
          "enabled": true,
          "direction": "LR",
          "sortMethod": "directed",
          "nodeSpacing": 200,
          "levelSeparation": 220
        }
      },
      "physics": { "enabled": false },
      "edges": {
        "smooth": { "type": "cubicBezier", "forceDirection": "horizontal", "roundness": 0.35 },
        "arrows": { "to": { "enabled": true } }
      }
    }
    """)

    def shorten(label: str, max_len: int = 40) -> str:
        s = (label or "").replace("market for ", "")
        s = " ".join(s.split())
        return s if len(s) <= max_len else s[: max_len - 1] + "…"

    for key, attrs in G.nodes(data=True):
        full = attrs.get("label") or f"{key[0]}::{key[1]}"
        kind = attrs.get("kind", "node")
        shape = "box" if kind in {"foreground", "root"} else "ellipse"
        net.add_node(
            str(key),
            label=shorten(full),
            title=full,   # full label on hover
            shape=shape,
        )

    for u, v, attrs in G.edges(data=True):
        # Put amounts/units on hover instead of on-canvas labels (reduces clutter)
        edge_txt = attrs.get("label", "")
        net.add_edge(str(u), str(v), title=edge_txt)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(out_path), open_browser=False, notebook=False)


def run_lcia_and_graph(root_act, methods: List[Tuple[str, str, str]]) -> None:
    demand = {root_act: float(ROOT_ACTIVITY_AMOUNT)}

    for method in methods:
        lca = bc.LCA(demand, method)
        lca.lci()
        lca.lcia()
        score = float(lca.score)

        # Graph (model-level)
        G = build_model_graph_from_foreground(root_act.key, depth=GRAPH_MAX_DEPTH)

        safe_method = "__".join([slugify(x) for x in method])
        safe_root = slugify(ROOT_ACTIVITY_NAME)
        out_html = GRAPHS_DIR / f"{safe_root}__{safe_method}__graph.html"

        export_graph_html(G, out_html, title=f"{ROOT_ACTIVITY_NAME} | {method} | score={score:g}")
        print(f"     ✓ {method} score={score:g} -> {out_html}")


def sanity_check_method_datapackages(method: Tuple[str, str, str]) -> None:
    """
    Optional: verify that the method zip contains datapackage.json.
    (Useful if you ever saw "There is no item named datapackage.json in the archive".)
    """
    m = bd.Method(method)
    token = getattr(m, "filename", None)
    if not token:
        return
    base = Path(bd.projects.dir)
    matches = list(base.glob(f"**/*{token}*.zip"))
    for p in matches[:3]:
        try:
            with zipfile.ZipFile(p) as z:
                if "datapackage.json" not in z.namelist():
                    raise RuntimeError(f"Corrupt method archive: {p}")
        except zipfile.BadZipFile as e:
            raise RuntimeError(f"Bad zip for method: {p}") from e


# ---------------------------
# Main
# ---------------------------

def main() -> None:
    print("[0/5] Project")
    set_project()
    ensure_dirs()
    print(f"     ✓ Project: {bd.projects.current}")
    print(f"     ✓ BW dir : {bd.projects.dir}")

    print("[1/5] Ecoinvent check")
    has_ei = try_install_ecoinvent()
    bg_index = build_ecoinvent_index() if has_ei else None

    print("[2/5] Parse Excel + build DBs")
    activities, exchanges = parse_lci_xlsx(XLSX_PATH, XLSX_SHEET)
    print(f"     ✓ Parsed {len(activities)} activities and {len(exchanges)} exchanges from {XLSX_PATH}")

    # Ensure biosphere exists if biosphere exchanges are present
    if any(str(ex.get("type", "")).strip().lower() == "biosphere" for ex in exchanges):
        if "biosphere3" not in bd.databases or len(list(bd.Database("biosphere3"))) == 0:
            raise RuntimeError("biosphere3 missing/empty. Run your bootstrap first (biosphere3 + LCIA methods).")

    fg_data, ext_data = build_foreground_and_external(activities, exchanges, bg_index=bg_index)
    print(f"     ✓ Foreground datasets: {len(fg_data)}")
    print(f"     ✓ External stub datasets: {len(ext_data)}")

    print("[3/5] Write databases")
    write_databases(fg_data, ext_data)
    print(f"     ✓ Wrote '{FOREGROUND_DB}' and '{EXTERNAL_DB}'")

    print("[4/5] Load methods + run LCIA")
    methods = load_methods_from_csv(METHODS_CSV)
    # Optional quick datapackage sanity check for the first method
    sanity_check_method_datapackages(methods[0])

    root_act = get_activity_by_name(FOREGROUND_DB, ROOT_ACTIVITY_NAME)
    run_lcia_and_graph(root_act, methods)

    print("[5/5] Done")
    print(f"     ✓ Graphs saved to: {GRAPHS_DIR}/")


if __name__ == "__main__":
    main()
