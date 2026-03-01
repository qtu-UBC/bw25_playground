"""Rule-based extraction for a small EPD-like text snippet.

This module is intentionally deterministic and lightweight. In a production
workflow, `extract_entities` is the place where an LLM or hybrid extractor
could be swapped in, while keeping downstream graph-writing behavior stable.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List

DEMO_TEXT = (
    "Electricity from the BC grid is used for kiln drying. "
    "Phenol-resorcinol-formaldehyde adhesive is applied during panel layup. "
    "Global warming potential is reported for modules A1-A3 in kg CO2e per m3 CLT."
)


@dataclass(frozen=True)
class ExtractedEntity:
    entity_type: str
    raw_text: str
    normalized: str


def normalize(text: str) -> str:
    lower = text.lower().strip()
    cleaned = re.sub(r"[^a-z0-9]+", " ", lower)
    return " ".join(cleaned.split())


def extract_entities(text: str) -> List[ExtractedEntity]:
    """Extract expected entities from the fixed EPD-like snippet.

    The patterns below are intentionally explicit for demo clarity.
    """
    entities: List[ExtractedEntity] = []
    seen = set()

    pattern_specs = [
        ("Energy", r"\bbc grid electricity\b|\belectricity from the bc grid\b"),
        ("Process", r"\bkiln drying\b"),
        (
            "Material",
            r"\bphenol-resorcinol-formaldehyde adhesive\b|\bphenol resorcinol formaldehyde adhesive\b",
        ),
        ("Indicator", r"\bglobal warming potential\b"),
        ("LifeCycleStage", r"\ba1-a3\b"),
        ("Unit", r"\bkg co2e per m3\b"),
        ("Product", r"\bclt\b"),
    ]

    normalized_text = normalize(text)

    for entity_type, pattern in pattern_specs:
        match = re.search(pattern, normalized_text, flags=re.IGNORECASE)
        if not match:
            continue

        raw = match.group(0)
        norm = normalize(raw)
        key = (entity_type, norm)
        if key in seen:
            continue

        entities.append(
            ExtractedEntity(
                entity_type=entity_type,
                raw_text=raw,
                normalized=norm,
            )
        )
        seen.add(key)

    return entities


if __name__ == "__main__":
    for item in extract_entities(DEMO_TEXT):
        print(item)
