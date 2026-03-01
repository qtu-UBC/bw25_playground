"""Seed canonical LCA domain nodes for the linking demo."""

from __future__ import annotations

from typing import Iterable

from neo4j import Driver


DOMAIN_NODES = [
    {
        "label": "ProductCategory",
        "id": "CLT",
        "canonical_name": "Cross-laminated timber",
    },
    {
        "label": "ElectricityMarket",
        "id": "CA-BC-grid",
        "canonical_name": "Electricity, medium voltage, BC, Canada",
    },
    {
        "label": "UnitProcess",
        "id": "PROC_KILN_DRYING",
        "canonical_name": "kiln drying",
    },
    {
        "label": "MaterialMaster",
        "id": "ADH_PRF",
        "canonical_name": "Phenol-resorcinol-formaldehyde (PRF) adhesive",
    },
    {
        "label": "LCIAIndicator",
        "id": "GWP100",
        "canonical_name": "Global warming potential",
        "method_family": "TRACI 2.1",
    },
    {
        "label": "ModuleSet",
        "id": "EN15804_A1_A3",
        "canonical_name": "Product stage (A1-A3)",
    },
    {
        "label": "DeclaredUnitPattern",
        "id": "KGCO2E_PER_M3",
        "canonical_name": "kg CO2e per m3",
    },
]


def seed_domain_graph(driver: Driver) -> None:
    with driver.session() as session:
        for node in DOMAIN_NODES:
            session.execute_write(_upsert_domain_node, node)


def _upsert_domain_node(tx, node: dict) -> None:
    label = node["label"]
    props = _props_fragment(node.keys())
    query = f"""
    MERGE (n:{label} {{id: $id}})
    SET {props}
    """
    tx.run(query, **node)


def _props_fragment(keys: Iterable[str]) -> str:
    pairs = [f"n.{key} = ${key}" for key in keys if key != "label"]
    return ", ".join(pairs)
