"""
zip_repair.py

Utility functions for extracting and repairing ZIP codes from messy address strings.

Primary use case:
- Run BEFORE libpostal parsing.
- If a trailing 4-digit "ZIP" is present (common in RI/MA when the leading zero is dropped),
  convert it to a 5-digit ZIP by padding a leading zero, then REMOVE it from the string
  that you send to libpostal.

Example:
    raw = "2 Old Walcott Ave, Jamestown RI 2835 USA"
    cleaned, zip5 = extract_and_repair_zip_ri_ma(raw)
    # cleaned -> "2 Old Walcott Ave, Jamestown RI USA" (or without USA depending on input)
    # zip5    -> "02835"

Notes:
- This is intentionally conservative to avoid mistaking "PO Box 2835" or "Apt 2835" as a ZIP.
- If you want additional states, you can generalize patterns (or add more functions).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple


# -----------------------------
# Configuration / Patterns
# -----------------------------

# Unit designators that often precede a number that is NOT a ZIP.
# We avoid interpreting a trailing 4-digit token as ZIP if it looks like a unit number.
_UNIT_WORDS_PATTERN = r"(?:apt|apartment|unit|ste|suite|#|fl|floor|bldg|building)\.?"

# PO Box pattern: we avoid treating the following number as ZIP.
_PO_BOX_PATTERN = r"(?:p\.?\s*o\.?\s*box|po\s*box)"

# Optional country strings sometimes appear at end.
_COUNTRY_PATTERN = r"(?:USA|US|United\s+States(?:\s+of\s+America)?)\.?"

# RI/MA state tokens.
_RI_MA_STATE_PATTERN = r"(?:RI|MA|Rhode\s+Island|Massachusetts)"


@dataclass(frozen=True)
class ZipRepairResult:
    """
    Result container for ZIP extraction/repair.

    cleaned_address:
        The original string with ZIP token removed (and punctuation/spacing cleaned).

    zip5:
        Repaired/extracted 5-digit ZIP as a string, or None if none found.

    zip_source:
        Describes which heuristic matched (useful for debugging/tests).
    """
    cleaned_address: str
    zip5: Optional[str]
    zip_source: Optional[str] = None


# -----------------------------
# Public API
# -----------------------------

def extract_and_repair_zip_ri_ma(address: str) -> Tuple[str, Optional[str]]:
    """
    Convenience wrapper returning (cleaned_address, zip5_or_none).

    Equivalent to:
        res = repair_zip_ri_ma(address)
        return res.cleaned_address, res.zip5
    """
    res = repair_zip_ri_ma(address)
    return res.cleaned_address, res.zip5


def repair_zip_ri_ma(address: str) -> ZipRepairResult:
    """
    Extract a ZIP from an address string, repairing RI/MA 4-digit ZIPs by padding a leading '0'.

    Heuristics (in this order):
      1) If a 5-digit ZIP (or ZIP+4) is present anywhere -> extract the 5-digit ZIP and remove token.
      2) If a 4-digit token is at the very end (optionally before country) -> treat as ZIP and pad,
         unless it looks like a unit number or PO Box.
      3) If pattern "... <RI/MA state> <4-digits> ..." -> treat those 4 digits as ZIP and pad,
         unless it looks like a unit number or PO Box.
      4) If pattern "... <4-digits> <RI/MA state> ..." -> treat those 4 digits as ZIP and pad,
         unless it looks like a unit number or PO Box.

    Returns ZipRepairResult with:
      - cleaned_address: address string with ZIP token removed
      - zip5: extracted/repaired 5-digit ZIP or None
      - zip_source: "zip5", "zip4_trailing", "zip4_after_state", or "zip4_before_state" for debugging

    This is designed to run before libpostal:
      - feed cleaned_address to libpostal
      - attach zip5 back into your parsed dict if present
    """
    if address is None:
        return ZipRepairResult(cleaned_address="", zip5=None, zip_source=None)

    s = str(address).strip()
    if not s:
        return ZipRepairResult(cleaned_address=s, zip5=None, zip_source=None)

    # ---- 1) Find 5-digit ZIP (or ZIP+4) anywhere ----
    # Example matches:
    #   02835
    #   02835-1234
    m = re.search(r"\b(\d{5})(?:-\d{4})?\b", s)
    if m:
        zip5 = m.group(1)
        # Replace with the normalized 5-digit ZIP in-place.
        cleaned = _replace_span_and_cleanup(s, m.span(), zip5)
        return ZipRepairResult(cleaned_address=cleaned, zip5=zip5, zip_source="zip5")

    # ---- 2) Trailing 4-digit ZIP (optionally followed by country) ----
    # Example:
    #   "... RI 2835"
    #   "... 2835 USA"
    m = re.search(
        rf"\b(\d{{4}})\b(?:\s*{_COUNTRY_PATTERN})?\s*$",
        s,
        flags=re.IGNORECASE,
    )
    if m:
        # Check what precedes the 4-digit token to avoid false positives.
        before = s[: m.start()].rstrip()

        if not _looks_like_unit_number_context(before) and not _looks_like_po_box_context(before):
            zip5 = "0" + m.group(1)
            cleaned = _replace_span_and_cleanup(s, m.span(), zip5)
            return ZipRepairResult(cleaned_address=cleaned, zip5=zip5, zip_source="zip4_trailing")

    # ---- 3) State + 4-digit token (anywhere) ----
    # Example:
    #   "... Jamestown RI 2835"
    #   "... Jamestown, Rhode Island 2835"
    m = re.search(
        rf"\b({_RI_MA_STATE_PATTERN})\b\W*(\d{{4}})\b",
        s,
        flags=re.IGNORECASE,
    )
    if m:
        before_digits = s[: m.start(2)].rstrip()

        # Avoid "Apt 2835" or "PO Box 2835"
        if not _looks_like_unit_number_context(before_digits) and not _looks_like_po_box_context(before_digits):
            zip5 = "0" + m.group(2)
            # Replace just the 4-digit token with the 5-digit ZIP; keep the state text.
            cleaned = _replace_span_and_cleanup(s, (m.start(2), m.end(2)), zip5)
            return ZipRepairResult(cleaned_address=cleaned, zip5=zip5, zip_source="zip4_after_state")

    # ---- 4) 4-digit token + State (anywhere) ----
    # Example:
    #   "... Barrington 2806 RI"
    #   "... Barrington, 2806, Rhode Island"
    m = re.search(
        rf"\b(\d{{4}})\b\W*\b({_RI_MA_STATE_PATTERN})\b",
        s,
        flags=re.IGNORECASE,
    )
    if m:
        before_digits = s[: m.start(1)].rstrip()

        # Avoid "Apt 2835 RI" or "PO Box 2835 RI"
        if not _looks_like_unit_number_context(before_digits) and not _looks_like_po_box_context(before_digits):
            zip5 = "0" + m.group(1)
            cleaned = _replace_span_and_cleanup(s, (m.start(1), m.end(1)), zip5)
            return ZipRepairResult(cleaned_address=cleaned, zip5=zip5, zip_source="zip4_before_state")

    # No ZIP found / repaired
    return ZipRepairResult(cleaned_address=s, zip5=None, zip_source=None)


# -----------------------------
# Internal helpers
# -----------------------------

def _looks_like_unit_number_context(text_before_number: str) -> bool:
    """
    Returns True if the text immediately before a number looks like unit/suite/apartment context.

    Example:
      "123 Main St Apt" -> True (so "Apt 2835" should not become a ZIP)
    """
    return bool(re.search(rf"{_UNIT_WORDS_PATTERN}\s*$", text_before_number, flags=re.IGNORECASE))


def _looks_like_po_box_context(text_before_number: str) -> bool:
    """
    Returns True if the text immediately before a number looks like PO Box context.

    Example:
      "PO Box" -> True (so "PO Box 2835" should not become a ZIP)
    """
    return bool(re.search(rf"{_PO_BOX_PATTERN}\s*$", text_before_number, flags=re.IGNORECASE))


def _remove_span_and_cleanup(text: str, span: Tuple[int, int]) -> str:
    """
    Remove text[span[0]:span[1]] and then clean up extra punctuation/spaces.

    This is intentionally simple (and predictable):
      - remove the span
      - collapse repeated spaces
      - trim leading/trailing commas and whitespace
    """
    a, b = span
    cleaned = (text[:a] + text[b:]).strip()

    # Clean up leftover double spaces
    cleaned = re.sub(r"\s{2,}", " ", cleaned)

    # Trim stray punctuation at ends
    cleaned = cleaned.strip(" ,;")

    # Also remove accidental " ,"
    cleaned = re.sub(r"\s+,", ",", cleaned)

    # Collapse spaces around commas
    cleaned = re.sub(r"\s*,\s*", ", ", cleaned).strip()

    return cleaned


def _replace_span_and_cleanup(text: str, span: Tuple[int, int], replacement: str) -> str:
    """
    Replace text[span[0]:span[1]] with replacement and then clean up punctuation/spaces.
    """
    a, b = span
    cleaned = (text[:a] + replacement + text[b:]).strip()

    # Clean up leftover double spaces
    cleaned = re.sub(r"\s{2,}", " ", cleaned)

    # Trim stray punctuation at ends
    cleaned = cleaned.strip(" ,;")

    # Also remove accidental " ,"
    cleaned = re.sub(r"\s+,", ",", cleaned)

    # Collapse spaces around commas
    cleaned = re.sub(r"\s*,\s*", ", ", cleaned).strip()

    return cleaned

'''
# -----------------------------
# Quick manual test (optional)
# -----------------------------
if __name__ == "__main__":
    samples = [
        "2 Old Walcott Ave, Jamestown RI 2835 USA",
        "2 Old Walcott Ave Jamestown RI 02835",
        "PO Box 2835, Jamestown RI",
        "123 Main St Apt 2835, Providence RI",
        "55 Bay View Ave, Jamestown, Rhode Island 2835",
        "Jamestown, 2835",
        "Jamestown, RI 2835",
    ]

    for s in samples:
        res = repair_zip_ri_ma(s)
        print(f"\nRAW:     {s}")
        print(f"CLEANED: {res.cleaned_address}")
        print(f"ZIP5:    {res.zip5}   ({res.zip_source})")
'''
