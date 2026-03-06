
"""
bw25_playground - main.py (xlsx + model-level graph)
====================================================

This script:
  1) Ensures Brightway project exists (assumes you ran your bootstrap for biosphere3 + LCIA methods)
  2) Imports foreground systems from all Excel files in input/activity tables
     into database "my_db" (foreground activities defined by "Activity" blocks in the sheet).
  3) Uses a TWO-DB approach for technosphere inputs that are not defined as foreground activities:
        - If ecoinvent-3.10.1-cutoff is installed, try to match inputs to ecoinvent.
        - Otherwise (or if no match), create a stub activity in "external_inputs".
  4) Runs LCIA for each file's model functional unit(s) (preferred ROOT_ACTIVITY_NAME plus inferred roots),
     for each method listed in input/methods.csv (rows are method tuples: level_1, level_2, level_3).
  5) Generates model-level supply-chain graphs (HTML) under output/graphs/.

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
from datetime import datetime
import traceback
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Set

import pandas as pd
import numpy as np
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
XLSX_DIR = Path("input") / "activity tables"
# If None, use the first sheet in each workbook.
XLSX_SHEET: Optional[str] = None

# Model root (functional unit)
ROOT_ACTIVITY_NAME = "carbon fiber production, weaved, at factory"
ROOT_ACTIVITY_AMOUNT = 1.0

# Methods list (CSV with columns: level_1, level_2, level_3)
METHODS_CSV = Path("input") / "methods.csv"

# Output
OUTPUT_DIR = Path("output")
GRAPHS_DIR = OUTPUT_DIR / "graphs"
LOGS_DIR = OUTPUT_DIR / "logs"
BIOSPHERE_SKIP_LOG = LOGS_DIR / "biosphere_skips.log"
ERROR_LOG = LOGS_DIR / "errors.log"

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


def as_text(value: Any, default: str = "") -> str:
    """Normalize spreadsheet values to safe strings."""
    if value is None:
        return default
    if isinstance(value, float) and pd.isna(value):
        return default
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return default
    return text


def ensure_dirs() -> None:
    GRAPHS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


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

def parse_lci_xlsx(xlsx_path: Path, sheet_name: Optional[str] = None) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
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

    if sheet_name is None:
        df = pd.read_excel(xlsx_path, header=None)
    else:
        try:
            df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None)
        except ValueError:
            # Fallback to first sheet if the requested sheet is not present.
            df = pd.read_excel(xlsx_path, header=None)

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
            "comment": as_text(find_value(block, "comment")),
            "source": as_text(find_value(block, "source")),
            "location": as_text(find_value(block, "location"), default="GLO"),
            "production_amount": find_value(block, "production amount"),
            "reference_product": as_text(find_value(block, "reference product"), default=act_name),
            "unit": as_text(find_value(block, "unit"), default="unit"),
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
                    "unit": as_text(r.get("unit", "")),
                    "type": ex_type,
                    "database": as_text(r.get("database", "")),
                    "location": as_text(r.get("location", "")),
                    "reference_product": as_text(r.get("reference product", "")),
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
    flow_name = as_text(flow_name)
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


def log_biosphere_skip(message: str) -> None:
    ts = datetime.now().isoformat(timespec="seconds")
    with BIOSPHERE_SKIP_LOG.open("a", encoding="utf-8") as f:
        f.write(f"[{ts}] {message}\n")


def log_error(message: str) -> None:
    ts = datetime.now().isoformat(timespec="seconds")
    with ERROR_LOG.open("a", encoding="utf-8") as f:
        f.write(f"[{ts}] {message}\n")


# ---------------------------
# Import to Brightway (two DBs)
# ---------------------------

def build_foreground_and_external(
    activities: Dict[str, Dict[str, Any]],
    exchanges: List[Dict[str, Any]],
    bg_index: Optional[Dict[Tuple[str, str, str], Tuple[str, str]]] = None,
    source_tag: str = "",
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
            "comment": as_text(meta.get("comment")),
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
            try:
                input_key = resolve_biosphere_flow(in_name)
            except ValueError as e:
                # Fallback: skip unknown biosphere flows and log details for data cleanup.
                log_biosphere_skip(
                    f"source={source_tag or 'unknown'} output_activity={ex['output_name']!r} "
                    f"input_flow={in_name!r} amount={amount} unit={unit!r} reason={e}"
                )
                continue

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
    try:
        bd.Database(FOREGROUND_DB).write(fg_data)
    except Exception as e:
        log_error(f"write_databases_failed error={e!r}\n{traceback.format_exc()}")
        raise


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


def discover_xlsx_files(xlsx_dir: Path) -> List[Path]:
    if not xlsx_dir.exists():
        raise FileNotFoundError(f"Input folder not found: {xlsx_dir}")
    files = sorted(
        [
            p
            for p in xlsx_dir.iterdir()
            if p.is_file()
            and p.suffix.lower() == ".xlsx"
            # Ignore temporary lock files created by Excel, e.g. "~$lci-trucks.xlsx".
            and not p.name.startswith("~$")
        ]
    )
    if not files:
        raise FileNotFoundError(f"No .xlsx files found in: {xlsx_dir}")
    return files


def infer_root_activity_names(
    activities: Dict[str, Dict[str, Any]],
    exchanges: List[Dict[str, Any]],
    preferred_name: str,
) -> List[str]:
    selected: List[str] = []
    if preferred_name in activities:
        selected.append(preferred_name)

    # Candidate roots: activities that are never used as a technosphere input by another foreground activity.
    tech_inputs = {
        str(ex.get("input_name") or "").strip()
        for ex in exchanges
        if str(ex.get("type") or "").strip().lower() == "technosphere"
    }
    roots = [name for name in activities.keys() if name not in tech_inputs]
    for name in roots:
        if name not in selected:
            selected.append(name)

    if selected:
        if preferred_name not in activities:
            print(f"     [info] Root '{preferred_name}' not found; inferred {len(selected)} root activity(ies).")
        elif len(selected) > 1:
            print(f"     [info] Including multiple roots in one graph: {len(selected)} activities.")
        return selected

    # Final fallback: first activity in file.
    fallback = next(iter(activities.keys()))
    print(f"     [info] Could not infer root from links; using first activity: {fallback!r}")
    return [fallback]


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


def build_model_graph_from_foreground(root_keys: List[Tuple[str, str]], depth: int = GRAPH_MAX_DEPTH) -> nx.DiGraph:
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
    for root_key in root_keys:
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

    def fmt(v: Optional[float]) -> str:
        if v is None:
            return "n/a"
        return f"{v:.6g}"

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
        impact_val = attrs.get("impact_value")
        impact_txt = f"impact contribution: {fmt(impact_val)}"
        net.add_edge(
            str(u),
            str(v),
            title=edge_txt,
            exchange_title=edge_txt,
            impact_title=impact_txt,
            amount=abs(float(attrs.get("amount") or 0.0)),
            impact_value=float(impact_val) if impact_val is not None else 0.0,
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(out_path), open_browser=False, notebook=False)
    inject_graph_interactions(out_path)


def inject_graph_interactions(html_path: Path) -> None:
    """Add click-to-focus behavior in exported vis.js HTML graphs."""
    if not html_path.exists():
        return

    html = html_path.read_text(encoding="utf-8")
    # PyVis template can render duplicate centered <h1> blocks; keep only the first.
    heading_blocks = list(re.finditer(r"<center>\s*<h1>.*?</h1>\s*</center>", html, flags=re.DOTALL))
    if len(heading_blocks) > 1:
        rebuilt = []
        last = 0
        for i, m in enumerate(heading_blocks):
            rebuilt.append(html[last:m.start()])
            if i == 0:
                rebuilt.append(m.group(0))
            last = m.end()
        rebuilt.append(html[last:])
        html = "".join(rebuilt)

    marker = "</body>"
    if marker not in html:
        return

    script = """
<script type="text/javascript">
(function () {
  if (typeof network === "undefined" || typeof nodes === "undefined" || typeof edges === "undefined") {
    return;
  }

  var DIM_NODE = "rgba(180, 180, 180, 0.22)";
  var DIM_EDGE = "rgba(180, 180, 180, 0.16)";
  var HIGHLIGHT_EDGE = "rgba(226, 88, 34, 0.9)";
  var IMPACT_HIGHLIGHT_EDGE = "rgba(0, 0, 0, 0.95)";
  var HIGHLIGHT_NODE_BG = "rgba(255, 215, 0, 0.35)";
  var HIGHLIGHT_NODE_BORDER = "rgba(212, 160, 23, 1)";
  var SHARED_EDGE = "rgba(124, 58, 237, 0.9)";
  var SHARED_NODE_BG = "rgba(243, 232, 255, 0.95)";
  var SHARED_NODE_BORDER = "rgba(124, 58, 237, 1)";

  var allNodes = {};
  var allEdges = {};
  nodes.get().forEach(function (n) { allNodes[n.id] = n; });
  edges.get().forEach(function (e) { allEdges[e.id] = e; });

  var maxEdgeAmount = 0;
  var maxEdgeImpact = 0;
  Object.keys(allEdges).forEach(function (id) {
    var amount = parseFloat(allEdges[id].amount || 0);
    if (!isNaN(amount) && amount > maxEdgeAmount) {
      maxEdgeAmount = amount;
    }
    var impact = Math.abs(parseFloat(allEdges[id].impact_value || 0));
    if (!isNaN(impact) && impact > maxEdgeImpact) {
      maxEdgeImpact = impact;
    }
  });

  var thresholdPct = 0.1;
  var activeNode = null;
  var activeMode = "normal"; // normal | focus | shared
  var sharedButton = null;
  var edgeValueMode = "exchange"; // exchange | impact
  var sharedListWrap = null;
  var sharedListBody = null;

  function createSliderUi() {
    var box = document.createElement("div");
    box.style.position = "fixed";
    box.style.top = "14px";
    box.style.right = "14px";
    box.style.zIndex = "9999";
    box.style.background = "rgba(255, 255, 255, 0.96)";
    box.style.border = "1px solid #d1d5db";
    box.style.borderRadius = "8px";
    box.style.padding = "10px 12px";
    box.style.boxShadow = "0 6px 16px rgba(0, 0, 0, 0.12)";
    box.style.fontFamily = "Arial, sans-serif";
    box.style.fontSize = "12px";
    box.style.color = "#111827";
    box.style.width = "320px";

    var title = document.createElement("div");
    title.textContent = "Edge Threshold Filter";
    title.style.fontWeight = "600";
    title.style.marginBottom = "6px";
    box.appendChild(title);

    var val = document.createElement("div");
    val.id = "edge-threshold-label";
    val.style.marginBottom = "6px";
    val.textContent = "Show edges >= 0.1% of max exchange";
    box.appendChild(val);

    var controls = document.createElement("div");
    controls.style.display = "flex";
    controls.style.gap = "8px";
    controls.style.alignItems = "center";
    controls.style.width = "100%";

    var slider = document.createElement("input");
    slider.type = "range";
    slider.min = "0.1";
    slider.max = "100";
    slider.step = "0.1";
    slider.value = "0.1";
    slider.style.flex = "1";

    var percentInput = document.createElement("input");
    percentInput.type = "number";
    percentInput.min = "0.1";
    percentInput.max = "100";
    percentInput.step = "0.1";
    percentInput.value = "0.1";
    percentInput.style.width = "74px";
    percentInput.style.padding = "2px 4px";
    percentInput.style.border = "1px solid #d1d5db";
    percentInput.style.borderRadius = "4px";
    percentInput.title = "Type threshold percent";

    function clampThreshold(raw) {
      var x = parseFloat(raw);
      if (isNaN(x)) x = 0.1;
      if (x < 0.1) x = 0.1;
      if (x > 100) x = 100;
      return x;
    }

    function setThreshold(nextVal) {
      thresholdPct = clampThreshold(nextVal);
      slider.value = thresholdPct.toFixed(1);
      percentInput.value = thresholdPct.toFixed(1);
      var basis = edgeValueMode === "impact" ? "impact" : "exchange";
      val.textContent = "Show edges >= " + thresholdPct.toFixed(1) + "% of max " + basis;
      applyView();
    }

    slider.addEventListener("input", function () {
      setThreshold(slider.value);
    });
    percentInput.addEventListener("change", function () {
      setThreshold(percentInput.value);
    });
    percentInput.addEventListener("keyup", function (evt) {
      if (evt.key === "Enter") {
        setThreshold(percentInput.value);
      }
    });

    controls.appendChild(slider);
    controls.appendChild(percentInput);
    box.appendChild(controls);

    var actionRow = document.createElement("div");
    actionRow.style.marginTop = "8px";
    actionRow.style.display = "flex";
    actionRow.style.gap = "8px";
    actionRow.style.flexWrap = "wrap";

    sharedButton = document.createElement("button");
    sharedButton.type = "button";
    sharedButton.textContent = "Highlight Shared Nodes";
    sharedButton.style.padding = "6px 8px";
    sharedButton.style.border = "1px solid #9ca3af";
    sharedButton.style.borderRadius = "6px";
    sharedButton.style.background = "#f9fafb";
    sharedButton.style.cursor = "pointer";
    sharedButton.addEventListener("click", function () {
      if (activeMode === "shared") {
        activeMode = "normal";
        sharedButton.textContent = "Highlight Shared Nodes";
      } else {
        activeMode = "shared";
        activeNode = null;
        sharedButton.textContent = "Clear Shared Highlight";
      }
      applyView();
    });
    actionRow.appendChild(sharedButton);

    var modeWrap = document.createElement("div");
    modeWrap.style.display = "flex";
    modeWrap.style.alignItems = "center";
    modeWrap.style.gap = "4px";

    var modeLabel = document.createElement("span");
    modeLabel.textContent = "Edge value:";
    modeLabel.style.color = "#374151";
    modeWrap.appendChild(modeLabel);

    var modeSelect = document.createElement("select");
    modeSelect.style.border = "1px solid #d1d5db";
    modeSelect.style.borderRadius = "6px";
    modeSelect.style.padding = "4px";
    var optExchange = document.createElement("option");
    optExchange.value = "exchange";
    optExchange.textContent = "Exchange";
    var optImpact = document.createElement("option");
    optImpact.value = "impact";
    optImpact.textContent = "Impact";
    modeSelect.appendChild(optExchange);
    modeSelect.appendChild(optImpact);
    modeSelect.value = edgeValueMode;
    modeSelect.addEventListener("change", function () {
      edgeValueMode = modeSelect.value;
      var basis = edgeValueMode === "impact" ? "impact" : "exchange";
      val.textContent = "Show edges >= " + thresholdPct.toFixed(1) + "% of max " + basis;
      applyView();
    });
    modeWrap.appendChild(modeSelect);
    actionRow.appendChild(modeWrap);
    box.appendChild(actionRow);

    sharedListWrap = document.createElement("div");
    sharedListWrap.style.marginTop = "8px";
    sharedListWrap.style.display = "none";

    var sharedTitle = document.createElement("div");
    sharedTitle.textContent = "Shared Nodes";
    sharedTitle.style.fontWeight = "600";
    sharedTitle.style.marginBottom = "4px";
    sharedListWrap.appendChild(sharedTitle);

    var tableWrap = document.createElement("div");
    tableWrap.style.maxHeight = "220px";
    tableWrap.style.overflowY = "auto";
    tableWrap.style.border = "1px solid #e5e7eb";
    tableWrap.style.borderRadius = "6px";

    var table = document.createElement("table");
    table.style.width = "100%";
    table.style.borderCollapse = "collapse";
    table.style.fontSize = "11px";

    var thead = document.createElement("thead");
    var hr = document.createElement("tr");
    var h1 = document.createElement("th");
    h1.textContent = "#";
    h1.style.textAlign = "left";
    h1.style.padding = "4px 6px";
    h1.style.position = "sticky";
    h1.style.top = "0";
    h1.style.background = "#f9fafb";
    var h2 = document.createElement("th");
    h2.textContent = "Name";
    h2.style.textAlign = "left";
    h2.style.padding = "4px 6px";
    h2.style.position = "sticky";
    h2.style.top = "0";
    h2.style.background = "#f9fafb";
    hr.appendChild(h1);
    hr.appendChild(h2);
    thead.appendChild(hr);
    table.appendChild(thead);

    sharedListBody = document.createElement("tbody");
    table.appendChild(sharedListBody);
    tableWrap.appendChild(table);
    sharedListWrap.appendChild(tableWrap);
    box.appendChild(sharedListWrap);

    var help = document.createElement("div");
    help.style.marginTop = "6px";
    help.style.color = "#4b5563";
    help.textContent = "Click a node to focus its path. Click empty area to reset.";
    box.appendChild(help);

    document.body.appendChild(box);
  }

  function neighbors(nodeId, direction) {
    var keep = new Set([nodeId]);
    var stack = [nodeId];
    while (stack.length > 0) {
      var cur = stack.pop();
      var edgeIds = network.getConnectedEdges(cur);
      for (var i = 0; i < edgeIds.length; i++) {
        var e = allEdges[edgeIds[i]];
        if (!e || !edgePassThreshold(e)) continue;
        var next = null;
        if (direction === "upstream" && e.to === cur) next = e.from;
        if (direction === "downstream" && e.from === cur) next = e.to;
        if (next !== null && !keep.has(next)) {
          keep.add(next);
          stack.push(next);
        }
      }
    }
    return keep;
  }

  function edgePassThreshold(edge) {
    var value = 0;
    var maxValue = 0;
    if (edgeValueMode === "impact") {
      value = Math.abs(parseFloat(edge.impact_value || 0));
      if (isNaN(value)) value = 0;
      maxValue = maxEdgeImpact;
    } else {
      value = parseFloat(edge.amount || 0);
      if (isNaN(value)) value = 0;
      maxValue = maxEdgeAmount;
    }
    if (maxValue <= 0) return true;
    return value >= (thresholdPct / 100) * maxValue;
  }

  function focusedNodeSet(nodeId) {
    if (!nodeId) return null;
    var up = neighbors(nodeId, "upstream");
    var down = neighbors(nodeId, "downstream");
    var keep = new Set();
    up.forEach(function (x) { keep.add(x); });
    down.forEach(function (x) { keep.add(x); });
    return keep;
  }

  function sharedNodeSet() {
    var rootNodeId = null;
    Object.keys(allNodes).forEach(function (id) {
      var n = allNodes[id];
      if (n && n.label === "MODEL_ROOT") {
        rootNodeId = n.id;
      }
    });
    if (!rootNodeId) return null;

    var finalProducts = [];
    Object.keys(allEdges).forEach(function (id) {
      var e = allEdges[id];
      if (!e || !edgePassThreshold(e)) return;
      if (e.to === rootNodeId) {
        finalProducts.push(e.from);
      }
    });
    if (finalProducts.length === 0) return null;

    var shared = null;
    for (var i = 0; i < finalProducts.length; i++) {
      var chain = neighbors(finalProducts[i], "upstream");
      if (shared === null) {
        shared = new Set(chain);
      } else {
        shared = new Set(Array.from(shared).filter(function (x) { return chain.has(x); }));
      }
    }
    return shared;
  }

  function updateSharedList(keep) {
    if (!sharedListWrap || !sharedListBody) return;

    if (activeMode !== "shared") {
      sharedListWrap.style.display = "none";
      sharedListBody.innerHTML = "";
      return;
    }

    sharedListWrap.style.display = "block";
    sharedListBody.innerHTML = "";

    if (!keep || keep.size === 0) {
      var emptyRow = document.createElement("tr");
      var emptyCell = document.createElement("td");
      emptyCell.colSpan = 2;
      emptyCell.textContent = "No shared nodes at current threshold.";
      emptyCell.style.padding = "6px";
      emptyCell.style.color = "#6b7280";
      emptyCell.style.borderTop = "1px solid #f3f4f6";
      emptyRow.appendChild(emptyCell);
      sharedListBody.appendChild(emptyRow);
      return;
    }

    var names = Array.from(keep).map(function (id) {
      var n = allNodes[id];
      if (!n) return String(id);
      var full = (n.title || n.label || String(id));
      return String(full);
    }).sort(function (a, b) {
      return a.localeCompare(b);
    });

    for (var i = 0; i < names.length; i++) {
      var tr = document.createElement("tr");
      var c1 = document.createElement("td");
      c1.textContent = String(i + 1);
      c1.style.padding = "4px 6px";
      c1.style.borderTop = "1px solid #f3f4f6";
      c1.style.verticalAlign = "top";

      var c2 = document.createElement("td");
      c2.textContent = names[i];
      c2.style.padding = "4px 6px";
      c2.style.borderTop = "1px solid #f3f4f6";
      c2.style.verticalAlign = "top";
      c2.style.wordBreak = "break-word";

      tr.appendChild(c1);
      tr.appendChild(c2);
      sharedListBody.appendChild(tr);
    }
  }

  function applyView() {
    var keep = null;
    if (activeMode === "shared") {
      keep = sharedNodeSet();
    } else if (activeMode === "focus") {
      keep = focusedNodeSet(activeNode);
    }

    var nodeBg = activeMode === "shared" ? SHARED_NODE_BG : HIGHLIGHT_NODE_BG;
    var nodeBorder = activeMode === "shared" ? SHARED_NODE_BORDER : HIGHLIGHT_NODE_BORDER;
    var edgeColor = activeMode === "shared" ? SHARED_EDGE : HIGHLIGHT_EDGE;
    if (edgeValueMode === "impact") {
      edgeColor = IMPACT_HIGHLIGHT_EDGE;
    }

    var nodeUpdates = [];
    Object.keys(allNodes).forEach(function (id) {
      var original = allNodes[id];
      if (!keep) {
        nodeUpdates.push({
          id: original.id,
          color: original.color,
          font: original.font,
          hidden: original.hidden || false
        });
      } else if (keep.has(original.id)) {
        nodeUpdates.push({
          id: original.id,
          color: {
            background: nodeBg,
            border: nodeBorder,
            highlight: { background: nodeBg, border: nodeBorder }
          },
          font: { color: "#1f2937" },
          hidden: false
        });
      } else {
        nodeUpdates.push({
          id: original.id,
          color: {
            background: DIM_NODE,
            border: DIM_NODE,
            highlight: { background: DIM_NODE, border: DIM_NODE }
          },
          font: { color: "rgba(110, 110, 110, 0.45)" },
          hidden: false
        });
      }
    });

    var edgeUpdates = [];
    Object.keys(allEdges).forEach(function (id) {
      var original = allEdges[id];
      var passThreshold = edgePassThreshold(original);
      var onPath = !keep || (keep.has(original.from) && keep.has(original.to));
      var visible = passThreshold && onPath;
      var titleText = edgeValueMode === "impact"
        ? (original.impact_title || original.title || "")
        : (original.exchange_title || original.title || "");
      edgeUpdates.push({
        id: original.id,
        hidden: !visible,
        color: keep ? (visible ? edgeColor : DIM_EDGE) : original.color,
        width: keep ? (visible ? 2.2 : 1) : (original.width || 1),
        title: titleText
      });
    });

    nodes.update(nodeUpdates);
    edges.update(edgeUpdates);
    updateSharedList(keep);
  }

  createSliderUi();
  applyView();

  network.on("click", function (params) {
    if (!params.nodes || params.nodes.length === 0) {
      activeMode = "normal";
      activeNode = null;
      if (sharedButton) sharedButton.textContent = "Highlight Shared Nodes";
      applyView();
      return;
    }
    activeMode = "focus";
    activeNode = params.nodes[0];
    if (sharedButton) sharedButton.textContent = "Highlight Shared Nodes";
    applyView();
  });
})();
</script>
"""

    html = html.replace(marker, script + "\n" + marker)
    html_path.write_text(html, encoding="utf-8")


def run_lcia_and_graph(root_acts, root_names: List[str], source_tag: str, methods: List[Tuple[str, str, str]]) -> None:
    demand = {act: float(ROOT_ACTIVITY_AMOUNT) for act in root_acts}
    graph_root_keys = [act.key for act in root_acts]
    for method in methods:
        lca = bc.LCA(demand, method)
        lca.lci()
        lca.lcia()
        score = float(lca.score)

        # Graph (model-level)
        G = build_model_graph_from_foreground(graph_root_keys, depth=GRAPH_MAX_DEPTH)
        annotate_graph_with_edge_impacts(G, lca)

        safe_method = "__".join([slugify(x) for x in method])
        safe_root = "multi_root" if len(root_names) > 1 else slugify(root_names[0])
        safe_source = slugify(source_tag)
        out_html = GRAPHS_DIR / f"{safe_source}__{safe_root}__{safe_method}__graph.html"

        impact_name = method[2] if len(method) >= 3 else " / ".join(method)
        export_graph_html(G, out_html, title=f"{source_tag} | {impact_name}")
        print(f"     ✓ {method} score={score:g} -> {out_html}")


def annotate_graph_with_edge_impacts(G: nx.DiGraph, lca: bc.LCA) -> None:
    """
    Add a simple edge-level impact contribution estimate to graph edges.

    Approach:
    1) Compute direct characterized impact per activity from characterized inventory columns.
    2) Allocate each input activity's direct impact to outgoing graph edges in proportion to
       (exchange amount * consumer supply) / producer supply.
    """
    try:
        by_activity = np.asarray(lca.characterized_inventory.sum(axis=0)).ravel()
    except Exception:
        return

    supply = np.asarray(lca.supply_array).ravel()
    if by_activity.size == 0 or supply.size == 0:
        return

    activity_index = lca.dicts.activity
    direct_by_key: Dict[Tuple[str, str], float] = {}
    supply_by_key: Dict[Tuple[str, str], float] = {}

    for node in G.nodes:
        if not isinstance(node, tuple) or node[0] == "__model__":
            continue
        try:
            act = bd.get_activity(node)
            idx = activity_index[act.id]
            direct_by_key[node] = float(by_activity[idx])
            supply_by_key[node] = abs(float(supply[idx]))
        except Exception:
            continue

    for u, v, attrs in G.edges(data=True):
        if not (isinstance(u, tuple) and isinstance(v, tuple)):
            attrs["impact_value"] = None
            continue
        if u[0] == "__model__" or v[0] == "__model__":
            attrs["impact_value"] = None
            continue

        direct_u = direct_by_key.get(u)
        supply_u = supply_by_key.get(u, 0.0)
        supply_v = supply_by_key.get(v, 0.0)
        ex_amt = abs(float(attrs.get("amount") or 0.0))
        if direct_u is None or supply_u <= 0.0:
            attrs["impact_value"] = None
            continue

        required = ex_amt * supply_v
        share = (required / supply_u) if supply_u > 0 else 0.0
        attrs["impact_value"] = direct_u * share


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


def validate_lcia_ready(demand: Dict[Any, float], methods: List[Tuple[str, str, str]], checks: int = 3) -> None:
    """
    Fail fast if LCIA methods appear unlinked to biosphere flows in the current project.

    This catches a common broken-bootstrap state where methods exist, but
    characterization factors don't match the active biosphere identifiers.
    """
    if not methods:
        raise RuntimeError("No LCIA methods provided for validation.")

    to_check = methods[: max(1, min(checks, len(methods)))]
    last_error: Optional[Exception] = None

    for method in to_check:
        try:
            method_data = bd.Method(method).load()
            method_ids: Set[int] = set()
            for flow_key, _ in method_data:
                if isinstance(flow_key, int):
                    method_ids.add(flow_key)
                elif isinstance(flow_key, (tuple, list)) and len(flow_key) == 2:
                    try:
                        method_ids.add(bd.get_node(database=flow_key[0], code=flow_key[1]).id)
                    except Exception:
                        pass

            lca = bc.LCA(demand, method)
            lca.lci()
            lca.lcia()

            biosphere_ids = set(lca.dicts.biosphere.reversed.values())
            overlap = len(method_ids & biosphere_ids)
            char_nnz = int((lca.characterization_matrix != 0).sum())
            if overlap > 0 and char_nnz > 0:
                return
        except Exception as e:
            last_error = e

    detail = f" Last error: {last_error!r}" if last_error else ""
    raise RuntimeError(
        "LCIA methods appear unlinked or empty for this project/model "
        "(zero overlap with biosphere characterization factors). "
        "Recommended fix: rebuild project bootstrap (biosphere3 + default methods) "
        "using a known-good Brightway setup, then rerun main.py." + detail
    )


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

    print("[2/5] Discover input Excel files")
    xlsx_files = discover_xlsx_files(XLSX_DIR)
    print(f"     ✓ Found {len(xlsx_files)} file(s) in {XLSX_DIR}")

    print("[3/5] Load methods")
    methods = load_methods_from_csv(METHODS_CSV)
    # Optional quick datapackage sanity check for the first method
    sanity_check_method_datapackages(methods[0])

    print("[4/5] Build DBs + run LCIA/graphs for each file")
    log_biosphere_skip("=== New run started ===")
    log_error("=== New run started ===")
    for xlsx_path in xlsx_files:
        source_tag = xlsx_path.stem
        print(f"     -> Processing: {xlsx_path}")
        activities, exchanges = parse_lci_xlsx(xlsx_path, XLSX_SHEET)
        print(f"        ✓ Parsed {len(activities)} activities and {len(exchanges)} exchanges")

        # Ensure biosphere exists if biosphere exchanges are present
        if any(str(ex.get("type", "")).strip().lower() == "biosphere" for ex in exchanges):
            if "biosphere3" not in bd.databases or len(list(bd.Database("biosphere3"))) == 0:
                raise RuntimeError("biosphere3 missing/empty. Run your bootstrap first (biosphere3 + LCIA methods).")

        fg_data, ext_data = build_foreground_and_external(
            activities,
            exchanges,
            bg_index=bg_index,
            source_tag=source_tag,
        )
        print(f"        ✓ Foreground datasets: {len(fg_data)}")
        print(f"        ✓ External stub datasets: {len(ext_data)}")

        write_databases(fg_data, ext_data)
        print(f"        ✓ Wrote '{FOREGROUND_DB}' and '{EXTERNAL_DB}'")

        root_names = infer_root_activity_names(activities, exchanges, ROOT_ACTIVITY_NAME)
        root_acts = [get_activity_by_name(FOREGROUND_DB, name) for name in root_names]
        demand = {act: float(ROOT_ACTIVITY_AMOUNT) for act in root_acts}
        validate_lcia_ready(demand, methods)
        run_lcia_and_graph(root_acts, root_names=root_names, source_tag=source_tag, methods=methods)

    print("[5/5] Done")
    print(f"     ✓ Graphs saved to: {GRAPHS_DIR}/")


if __name__ == "__main__":
    main()
