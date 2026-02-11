from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import math
import re


@dataclass(frozen=True)
class SimpleCfg:
    # Accept a road if its max bbox dimension is <= this many meters
    max_linear_m: float = 1609.34  # 1 mile
    # Require at least this rank (keeps out towns/postcodes most of the time)
    min_place_rank: int = 26       # street-level and up; tighten to 28 later if you want


_CITY_LEVEL_KEYS = (
    "city",
    "town",
    "village",
)

_US_STATE_ABBR_TO_NAME: Dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
    "PR": "Puerto Rico",
    "GU": "Guam",
    "VI": "U.S. Virgin Islands",
    "AS": "American Samoa",
    "MP": "Northern Mariana Islands",
}


def _to_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def _normalize_zip5(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    match = re.search(r"\b(\d{5})(?:-\d{4})?\b", text)
    if not match:
        return ""
    return match.group(1)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().casefold()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


_STATE_NORMALIZED_TO_ABBR: Dict[str, str] = {
    _normalize_text(abbr): abbr for abbr in _US_STATE_ABBR_TO_NAME
}
_STATE_NORMALIZED_TO_ABBR.update(
    {_normalize_text(name): abbr for abbr, name in _US_STATE_ABBR_TO_NAME.items()}
)


def _normalize_state(value: Any) -> str:
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""

    # Nominatim may return ISO codes such as "US-RI" in ISO3166 keys.
    iso_match = re.search(r"\bUS[-\s]([A-Za-z]{2})\b", raw, flags=re.IGNORECASE)
    if iso_match:
        candidate = iso_match.group(1).upper()
        if candidate in _US_STATE_ABBR_TO_NAME:
            return candidate

    normalized = _normalize_text(raw)
    if not normalized:
        return ""
    mapped = _STATE_NORMALIZED_TO_ABBR.get(normalized)
    if mapped:
        return mapped
    if len(normalized) == 2:
        candidate = normalized.upper()
        if candidate in _US_STATE_ABBR_TO_NAME:
            return candidate
    return normalized


def _is_city_level_match(expected_town: str, value: Any) -> bool:
    expected_norm = _normalize_text(expected_town)
    candidate_norm = _normalize_text(value)
    if not expected_norm or not candidate_norm:
        return False
    if candidate_norm == expected_norm:
        return True
    return f" {expected_norm} " in f" {candidate_norm} "


def _city_match_from_address_levels(
    expected_town: str,
    address: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    matches: List[str] = []
    for key in _CITY_LEVEL_KEYS:
        value = address.get(key)
        if _is_city_level_match(expected_town, value):
            matches.append(key)
    return (len(matches) > 0), matches


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def _bbox_max_dim_m(bbox: Any) -> Optional[float]:
    """
    Nominatim JSONv2 boundingbox is typically:
      [south_lat, north_lat, west_lon, east_lon] as strings.
    """
    if not bbox or not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
        return None

    s_lat = _to_float(bbox[0])
    n_lat = _to_float(bbox[1])
    w_lon = _to_float(bbox[2])
    e_lon = _to_float(bbox[3])
    if None in (s_lat, n_lat, w_lon, e_lon):
        return None

    mid_lat = (s_lat + n_lat) / 2.0
    ns = _haversine_m(s_lat, w_lon, n_lat, w_lon)
    ew = _haversine_m(mid_lat, w_lon, mid_lat, e_lon)
    return max(ns, ew)


def _denylisted_class_type(res: Dict[str, Any]) -> bool:
    """
    Nominatim returns `class`/`type` representing the primary OSM tag.
    We reject obvious "area-ish" results.
    """
    c = res.get("class")
    t = res.get("type")

    if c == "boundary":
        return True

    # "place=postcode", "place=town", etc.
    if c == "place" and t in {"postcode", "city", "town", "village", "hamlet", "suburb", "neighbourhood"}:
        return True

    # If you want to accept roads, do NOT reject "highway" here.
    # If you later decide roads are too permissive, add: if c == "highway": return True
    return False


def nominatim_result_check(
    res: Dict[str, Any],
    expected_zip: str = "",
    expected_town: str = "",
    expected_state: str = "",
    cfg: SimpleCfg = SimpleCfg(),
) -> Tuple[bool, Optional[str], Optional[str], Dict[str, Any]]:
    """
    Minimal checker for one Nominatim JSONv2 candidate.

    Accepts:
      - address-level objects, POIs, AND short roads (<= max_linear_m)
    Rejects:
      - towns, postcodes, admin boundaries, etc.
      - anything with bbox longer than max_linear_m (prevents huge roads/regions)
    """
    
    reasons: List[str] = []
    reason_logic: List[str] = []
    diag: Dict[str, Any] = {}
    res_class = res.get("class")
    res_type = res.get("type")

    # Check 1: reject obvious broad classes/types
    if _denylisted_class_type(res):
        reasons.append("BROAD_CLASS_TYPE")
        reason_logic.append(
            f"class/type rejected for broad feature: class={res_class!r}, type={res_type!r}"
        )

    # Optional: use place_rank as an additional broadness gate
    pr = res.get("place_rank")
    try:
        pr_int = int(pr) if pr is not None else None
    except Exception:
        pr_int = None
    diag["place_rank"] = pr_int

    if pr_int is not None and pr_int < cfg.min_place_rank:
        reasons.append("PLACE_RANK_TOO_LOW")
        reason_logic.append(
            f"place_rank too low: place_rank={pr_int}, min_place_rank={cfg.min_place_rank}"
        )

    # Check 2: max bbox dimension <= threshold
    bbox_dim = _bbox_max_dim_m(res.get("boundingbox"))
    diag["bbox_max_dim_m"] = bbox_dim

    # If bbox missing, you can choose to be conservative or permissive.
    # Here: conservative reject, but you can flip this behavior.
    if bbox_dim is None:
        reasons.append("MISSING_BBOX")
        reason_logic.append("boundingbox missing or unparsable.")
    elif bbox_dim > cfg.max_linear_m:
        reasons.append("TOO_LONG_FEATURE")
        reason_logic.append(
            "feature bounding box too large: "
            f"bbox_max_dim_m={bbox_dim:.3f}, max_linear_m={cfg.max_linear_m:.3f}"
        )

    # Check 3: location consistency must match by ZIP OR (city-level component AND state).
    address = res.get("address") or {}
    expected_zip5 = _normalize_zip5(expected_zip)
    result_zip5 = _normalize_zip5(address.get("postcode"))
    zip_match = bool(expected_zip5 and result_zip5 and expected_zip5 == result_zip5)
    town_match, town_match_keys = _city_match_from_address_levels(expected_town, address)
    expected_town_norm = _normalize_text(expected_town)
    expected_state_norm = _normalize_state(expected_state)
    result_state = (
        address.get("state")
        or address.get("state_code")
        or address.get("ISO3166-2-lvl4")
        or address.get("ISO3166-2-lvl6")
    )
    result_state_norm = _normalize_state(result_state)
    state_match = bool(
        expected_state_norm and result_state_norm and expected_state_norm == result_state_norm
    )
    location_match = bool(zip_match or (town_match and state_match))

    diag["expected_zip5"] = expected_zip5 or None
    diag["result_zip5"] = result_zip5 or None
    diag["zip_match"] = zip_match
    diag["expected_city"] = expected_town or None
    diag["expected_city_normalized"] = expected_town_norm or None
    diag["city_match"] = town_match
    diag["city_match_keys"] = town_match_keys
    diag["expected_state"] = expected_state or None
    diag["expected_state_normalized"] = expected_state_norm or None
    diag["result_state"] = result_state or None
    diag["result_state_normalized"] = result_state_norm or None
    diag["state_match"] = state_match
    diag["location_match"] = location_match

    # Backward compatibility for existing metadata consumers.
    diag["expected_town"] = expected_town or None
    diag["expected_town_normalized"] = expected_town_norm or None
    diag["town_match"] = town_match
    diag["town_match_keys"] = town_match_keys

    if not location_match:
        reasons.append("ZIP_OR_CITY_STATE_MISMATCH")
        reason_logic.append(
            "zip/(city+state) consistency failed: "
            f"expected_zip5={expected_zip5!r}, result_zip5={result_zip5!r}, zip_match={zip_match}; "
            f"expected_city={expected_town!r}, expected_city_normalized={expected_town_norm!r}, "
            f"city_match={town_match}, city_match_keys={town_match_keys}; "
            f"expected_state={expected_state!r}, expected_state_normalized={expected_state_norm!r}, "
            f"result_state={result_state!r}, result_state_normalized={result_state_norm!r}, "
            f"state_match={state_match}; rule=zip_match or (city_match and state_match)"
        )

    accepted = (len(reasons) == 0)
    diag["reasons"] = reasons
    short_reason = ",".join(reasons) if reasons else None
    verbose_logic = " | ".join(reason_logic) if reason_logic else None
    return accepted, short_reason, verbose_logic, diag
