"""Run the LCA Neo4j subject-to-domain linking demo."""

from __future__ import annotations

import os
from pathlib import Path

from neo4j import GraphDatabase

from extract import DEMO_TEXT, extract_entities
from link import create_subject_graph, fetch_link_summary, link_subject_entities
from seed_domain import seed_domain_graph

DOC_ID = "demo-epd-1"


def load_env_from_file_if_needed(env_path: Path) -> None:
    """Load simple KEY=VALUE pairs from a local .env file if vars are missing."""
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key and key not in os.environ:
            os.environ[key] = value


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def cleanup_demo_subject_graph(driver) -> None:
    with driver.session() as session:
        session.run(
            """
            MATCH (d:SubjectDocument {id: $doc_id})-[:MENTIONS]->(e:SubjectEntity)
            DETACH DELETE e
            """,
            doc_id=DOC_ID,
        )
        session.run(
            """
            MATCH (d:SubjectDocument {id: $doc_id})
            DETACH DELETE d
            """,
            doc_id=DOC_ID,
        )


def print_summary(rows: list[dict]) -> None:
    print("\n=== Link Summary ===")
    for row in rows:
        rel = row["rel_type"] or "NO_MATCH"
        canonical_id = row["canonical_id"] or "-"
        canonical_name = row["canonical_name"] or "-"
        score = "-" if row["score"] is None else f"{row['score']:.2f}"
        method = row["method"] or "-"
        print(
            f"[{row['entity_type']}] '{row['raw_text']}' -> {rel} -> "
            f"{canonical_id} ({canonical_name}) | score={score} method={method}"
        )


def main() -> None:
    demo_dir = Path(__file__).resolve().parent
    load_env_from_file_if_needed(demo_dir / ".env")

    uri = require_env("NEO4J_URI")
    user = require_env("NEO4J_USER")
    password = require_env("NEO4J_PASSWORD")

    entities = extract_entities(DEMO_TEXT)

    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        cleanup_demo_subject_graph(driver)
        seed_domain_graph(driver)
        create_subject_graph(driver, DOC_ID, DEMO_TEXT, entities)
        link_subject_entities(driver, DOC_ID)
        summary_rows = fetch_link_summary(driver, DOC_ID)

    print("Demo text:")
    print(DEMO_TEXT)
    print_summary(summary_rows)


if __name__ == "__main__":
    main()
