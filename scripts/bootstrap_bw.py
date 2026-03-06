"""
Bootstrap Brightway project data for this repo.

This script ensures the active project has:
- biosphere3
- default LCIA methods
- a basic LCIA sanity check with non-zero characterization matrix

Run:
    uv run python scripts/bootstrap_bw.py
"""

from __future__ import annotations

from typing import Iterable, Tuple, Any, Set

import bw2data as bd
import bw2io as bi
import bw2calc as bc


PROJECT_NAME = "bw25_playground"
SMOKE_METHODS = [
    ("CML v4.8 2016", "acidification", "acidification (incl. fate, average Europe total, A&B)"),
    ("CML v4.8 2016", "eutrophication", "eutrophication (fate not incl.)"),
    ("ReCiPe 2016 v1.03, midpoint (H)", "water use", "water consumption potential (WCP)"),
]


def _method_ids(method: Tuple[str, str, str]) -> Set[int]:
    ids: Set[int] = set()
    for flow_key, _ in bd.Method(method).load():
        if isinstance(flow_key, int):
            ids.add(flow_key)
        elif isinstance(flow_key, (tuple, list)) and len(flow_key) == 2:
            try:
                ids.add(bd.get_node(database=flow_key[0], code=flow_key[1]).id)
            except Exception:
                pass
    return ids


def _install_default_methods_with_compat_patch() -> None:
    """
    Install default LCIA methods, handling bw2io/bw2data key-shape mismatch.

    bw2io 0.9.x can emit list flow keys while bw2data expects tuple/int.
    We patch Method.write for this run only.
    """
    from bw2data.method import Method

    original_write = Method.write

    def patched_write(self, data, process=True):  # type: ignore[override]
        fixed = []
        for line in data:
            if not line:
                continue
            flow = line[0]
            if isinstance(flow, list):
                flow = tuple(flow)
            if len(line) == 2:
                fixed.append((flow, line[1]))
            else:
                fixed.append((flow, *line[1:]))
        return original_write(self, fixed, process=process)

    Method.write = patched_write
    try:
        bi.create_default_lcia_methods(overwrite=True, shortcut=True)
    finally:
        Method.write = original_write


def ensure_project_bootstrap() -> None:
    bd.projects.set_current(PROJECT_NAME)
    print(f"[bootstrap] Project: {bd.projects.current}")

    if "biosphere3" not in bd.databases or len(bd.Database("biosphere3")) == 0:
        print("[bootstrap] Creating biosphere3 ...")
        bi.create_default_biosphere3(overwrite=True)
        print(f"[bootstrap] biosphere3 size: {len(bd.Database('biosphere3'))}")
    else:
        print(f"[bootstrap] biosphere3 already present (size={len(bd.Database('biosphere3'))})")

    print("[bootstrap] Installing default LCIA methods ...")
    _install_default_methods_with_compat_patch()
    print(f"[bootstrap] methods installed: {len(bd.methods)}")


def validate_lcia_linkage(methods: Iterable[Tuple[str, str, str]]) -> None:
    """Validate that at least one target method links to active biosphere ids."""
    # Use any existing technosphere activity if available.
    # Prefer user dbs, then fallback to ecoinvent.
    candidate = None
    for db_name in ("my_db", "ecoinvent-3.10.1-cutoff"):
        if db_name in bd.databases and len(bd.Database(db_name)):
            candidate = next(iter(bd.Database(db_name)))
            break

    # Fresh project fallback: create a tiny temporary technosphere db linked to biosphere3.
    temp_db_name = "__bootstrap_tmp__"
    used_temp_db = False
    if candidate is None:
        if temp_db_name in bd.databases:
            del bd.databases[temp_db_name]
        key = (temp_db_name, "tmp_activity")
        tmp_data = {
            key: {
                "name": "tmp bootstrap activity",
                "code": "tmp_activity",
                "database": temp_db_name,
                "location": "GLO",
                "unit": "unit",
                "reference product": "tmp bootstrap activity",
                "exchanges": [
                    {"input": key, "output": key, "amount": 1.0, "type": "production", "unit": "unit"},
                    {
                        "input": ("biosphere3", "9990b51b-7023-4700-bca0-1a32ef921f74"),
                        "output": key,
                        "amount": 1.0,
                        "type": "biosphere",
                        "unit": "kilogram",
                    },
                ],
            }
        }
        bd.Database(temp_db_name).write(tmp_data)
        candidate = bd.get_activity(key)
        used_temp_db = True

    try:
        demand = {candidate: 1.0}

        for method in methods:
            if method not in bd.methods:
                continue
            method_ids = _method_ids(method)
            if not method_ids:
                continue

            lca = bc.LCA(demand, method)
            lca.lci()
            lca.lcia()

            biosphere_ids = set(lca.dicts.biosphere.reversed.values())
            overlap = len(method_ids & biosphere_ids)
            char_nnz = int((lca.characterization_matrix != 0).sum())

            print(f"[bootstrap] method={method} overlap={overlap} char_nnz={char_nnz} score={float(lca.score):g}")
            if overlap > 0 and char_nnz > 0:
                print("[bootstrap] LCIA sanity check passed.")
                return

        raise RuntimeError(
            "LCIA sanity check failed: methods installed but no non-zero characterization linkage found. "
            "Check Brightway package compatibility and rebuild project bootstrap."
        )
    finally:
        if used_temp_db and temp_db_name in bd.databases:
            del bd.databases[temp_db_name]


def main() -> None:
    ensure_project_bootstrap()
    validate_lcia_linkage(SMOKE_METHODS)


if __name__ == "__main__":
    main()
