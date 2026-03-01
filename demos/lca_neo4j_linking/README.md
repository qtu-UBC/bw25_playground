# LCA Neo4j Linking Demo

Small, deterministic demo that:

1. extracts entities from one EPD-like text snippet
2. seeds a canonical LCA domain graph
3. links extracted subject entities to canonical nodes via normalized alias matching
4. prints a terminal summary of created links

## Demo input text

This demo uses the exact text below:

`Electricity from the BC grid is used for kiln drying. Phenol-resorcinol-formaldehyde adhesive is applied during panel layup. Global warming potential is reported for modules A1-A3 in kg CO2e per m3 CLT.`

## Prerequisites

- Python environment with `neo4j` installed (already true in this repo environment)
- Local Neo4j instance running

## Start/connect to local Neo4j

Option A: Neo4j Desktop

1. Start your local DBMS from Neo4j Desktop.
2. Note connection details (usually `bolt://localhost:7687`, user `neo4j`, your chosen password).

Option B: Docker

```bash
docker run --name neo4j-lca-demo \
  -p7474:7474 -p7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  -d neo4j:5
```

## Configure environment variables

From repository root:

```bash
cp demos/lca_neo4j_linking/.env.example demos/lca_neo4j_linking/.env
```

Edit `demos/lca_neo4j_linking/.env` and set:

- `NEO4J_URI` (for example `bolt://localhost:7687`)
- `NEO4J_USER` (for example `neo4j`)
- `NEO4J_PASSWORD` (your password)

The runner auto-loads `demos/lca_neo4j_linking/.env` if present.

## Run the demo

From repository root:

```bash
python demos/lca_neo4j_linking/run_demo.py
```

Expected extracted targets include:

- BC grid electricity
- kiln drying
- phenol-resorcinol-formaldehyde adhesive
- global warming potential
- A1-A3
- kg CO2e per m3
- CLT

## Inspect results in Neo4j

Example Cypher:

```cypher
MATCH (d:SubjectDocument {id: "demo-epd-1"})-[:MENTIONS]->(e:SubjectEntity)
OPTIONAL MATCH (e)-[r:REFERS_TO|CANDIDATE_MATCH]->(c)
RETURN d.id, e.entity_type, e.raw_text, e.normalized, type(r) AS rel, r.score, r.method, labels(c) AS c_labels, c.id, c.canonical_name
ORDER BY e.entity_type, e.normalized;
```

## Notes

- Linking is deterministic (`normalize + alias dictionary`) to keep behavior explainable.
- `extract.py` is deliberately modular so a future LLM extractor can replace rule-based extraction without changing graph/linking code.
