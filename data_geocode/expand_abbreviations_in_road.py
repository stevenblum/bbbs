"""
Expand common street abbreviations to full words.

This is intentionally conservative and only expands well-known abbreviations.
It is designed to run before fuzzy matching.
"""

from __future__ import annotations

import re


_ABBREVIATIONS = {
    "ave": "avenue",
    "av": "avenue",
    "blvd": "boulevard",
    "cir": "circle",
    "ct": "court",
    "ctr": "center",
    "cv": "cove",
    "dr": "drive",
    "expy": "expressway",
    "expwy": "expressway",
    "hwy": "highway",
    "ln": "lane",
    "pkwy": "parkway",
    "pl": "place",
    "rd": "road",
    "sq": "square",
    "st": "street",
    "ter": "terrace",
    "trl": "trail",
    "way": "way",
}


def expand_abbreviations_in_road(text: str) -> str:
    if not text:
        return ""

    # Replace punctuation with spaces, keep alphanumerics.
    cleaned = re.sub(r"[^A-Za-z0-9]+", " ", text)
    tokens = [t for t in cleaned.split() if t]
    expanded = [_ABBREVIATIONS.get(t.lower(), t) for t in tokens]
    return " ".join(expanded)
