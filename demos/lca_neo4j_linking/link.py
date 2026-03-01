"""Build subject graph nodes and link them to canonical domain nodes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from neo4j import Driver

from extract import ExtractedEntity


@dataclass(frozen=True)
class MatchResult:
    relationship_type: str
    canonical_label: str
    canonical_id: str
    score: float
    method: str


ALIAS_MATCHES: Dict[str, Dict[str, MatchResult]] = {
    "Product": {
        "clt": MatchResult("REFERS_TO", "ProductCategory", "CLT", 0.99, "abbreviation"),
    },
    "Energy": {
        "electricity from the bc grid": MatchResult(
            "REFERS_TO",
            "ElectricityMarket",
            "CA-BC-grid",
            0.97,
            "alias+location",
        ),
        "bc grid electricity": MatchResult(
            "REFERS_TO",
            "ElectricityMarket",
            "CA-BC-grid",
            0.97,
            "alias+location",
        ),
        "bc grid": MatchResult(
            "REFERS_TO",
            "ElectricityMarket",
            "CA-BC-grid",
            0.94,
            "alias+location",
        ),
        "electricity": MatchResult(
            "CANDIDATE_MATCH",
            "ElectricityMarket",
            "CA-BC-grid",
            0.55,
            "alias(no-location)",
        ),
    },
    "Process": {
        "kiln drying": MatchResult("REFERS_TO", "UnitProcess", "PROC_KILN_DRYING", 0.98, "alias"),
    },
    "Material": {
        "phenol resorcinol formaldehyde adhesive": MatchResult(
            "REFERS_TO",
            "MaterialMaster",
            "ADH_PRF",
            0.99,
            "alias",
        ),
    },
    "Indicator": {
        "global warming potential": MatchResult(
            "REFERS_TO",
            "LCIAIndicator",
            "GWP100",
            0.99,
            "ontology",
        ),
    },
    "LifeCycleStage": {
        "a1 a3": MatchResult("REFERS_TO", "ModuleSet", "EN15804_A1_A3", 0.99, "ontology"),
    },
    "Unit": {
        "kg co2e per m3": MatchResult(
            "REFERS_TO",
            "DeclaredUnitPattern",
            "KGCO2E_PER_M3",
            0.99,
            "ontology",
        ),
    },
}


def create_subject_graph(
    driver: Driver,
    doc_id: str,
    text: str,
    entities: List[ExtractedEntity],
) -> None:
    with driver.session() as session:
        session.execute_write(_merge_document, doc_id, text)
        for entity in entities:
            session.execute_write(_merge_entity, doc_id, entity)


def _merge_document(tx, doc_id: str, text: str) -> None:
    tx.run(
        """
        MERGE (d:SubjectDocument {id: $doc_id})
        SET d.text = $text
        """,
        doc_id=doc_id,
        text=text,
    )


def _merge_entity(tx, doc_id: str, entity: ExtractedEntity) -> None:
    tx.run(
        """
        MATCH (d:SubjectDocument {id: $doc_id})
        MERGE (e:SubjectEntity {
          doc_id: $doc_id,
          entity_type: $entity_type,
          normalized: $normalized
        })
        SET e.raw_text = $raw_text
        MERGE (d)-[:MENTIONS]->(e)
        """,
        doc_id=doc_id,
        entity_type=entity.entity_type,
        normalized=entity.normalized,
        raw_text=entity.raw_text,
    )


def link_subject_entities(driver: Driver, doc_id: str) -> None:
    with driver.session() as session:
        rows = session.execute_read(_read_subject_entities, doc_id)
        for row in rows:
            match = resolve_match(row["entity_type"], row["normalized"])
            if not match:
                continue
            session.execute_write(
                _merge_link,
                row["node_id"],
                match.relationship_type,
                match.canonical_label,
                match.canonical_id,
                match.score,
                match.method,
            )


def _read_subject_entities(tx, doc_id: str) -> List[dict]:
    result = tx.run(
        """
        MATCH (:SubjectDocument {id: $doc_id})-[:MENTIONS]->(e:SubjectEntity)
        RETURN id(e) AS node_id, e.entity_type AS entity_type, e.normalized AS normalized
        ORDER BY e.entity_type, e.normalized
        """,
        doc_id=doc_id,
    )
    return [dict(r) for r in result]


def resolve_match(entity_type: str, normalized: str) -> Optional[MatchResult]:
    return ALIAS_MATCHES.get(entity_type, {}).get(normalized)


def _merge_link(
    tx,
    subject_node_id: int,
    relationship_type: str,
    canonical_label: str,
    canonical_id: str,
    score: float,
    method: str,
) -> None:
    query = f"""
    MATCH (e) WHERE id(e) = $subject_node_id
    MATCH (d:{canonical_label} {{id: $canonical_id}})
    MERGE (e)-[r:{relationship_type}]->(d)
    SET r.score = $score, r.method = $method
    """
    tx.run(
        query,
        subject_node_id=subject_node_id,
        canonical_id=canonical_id,
        score=score,
        method=method,
    )


def fetch_link_summary(driver: Driver, doc_id: str) -> List[dict]:
    with driver.session() as session:
        result = session.execute_read(_read_link_summary, doc_id)
    return result


def _read_link_summary(tx, doc_id: str) -> List[dict]:
    result = tx.run(
        """
        MATCH (:SubjectDocument {id: $doc_id})-[:MENTIONS]->(e:SubjectEntity)
        OPTIONAL MATCH (e)-[r:REFERS_TO|CANDIDATE_MATCH]->(d)
        RETURN
          e.entity_type AS entity_type,
          e.raw_text AS raw_text,
          e.normalized AS normalized,
          type(r) AS rel_type,
          labels(d)[0] AS canonical_label,
          d.id AS canonical_id,
          d.canonical_name AS canonical_name,
          r.score AS score,
          r.method AS method
        ORDER BY entity_type, normalized
        """,
        doc_id=doc_id,
    )
    return [dict(r) for r in result]
