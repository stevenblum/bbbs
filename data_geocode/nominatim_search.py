"""
Nominatim search helper that parses raw addresses with libpostal, runs
Nominatim queries, and stores results plus debug logs on the instance.
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import threading
import time
from typing import Any, Optional, Dict

import requests
from requests import exceptions as requests_exceptions
try:
    from rapidfuzz import fuzz as rf_fuzz
    from rapidfuzz import process as rf_process
    from rapidfuzz import utils as rf_utils
except Exception:  # pragma: no cover - optional dependency
    rf_fuzz = None
    rf_process = None
    rf_utils = None
try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency
    psycopg = None
try:
    import usaddress
except Exception:  # pragma: no cover - optional dependency
    usaddress = None


def _bootstrap_libpostal_library_path() -> None:
    if not sys.platform.startswith("linux"):
        return
    if os.environ.get("_LIBPOSTAL_BOOTSTRAPPED") == "1":
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    libpostal_libdir = os.path.abspath(
        os.path.join(script_dir, "..", "libpostal", "src", ".libs")
    )
    if not os.path.isdir(libpostal_libdir):
        return

    current = os.environ.get("LD_LIBRARY_PATH", "")
    paths = [p for p in current.split(":") if p] if current else []
    if libpostal_libdir not in paths:
        os.environ["LD_LIBRARY_PATH"] = (
            f"{libpostal_libdir}:{current}" if current else libpostal_libdir
        )
        os.environ["_LIBPOSTAL_BOOTSTRAPPED"] = "1"
        reexec_argv = getattr(sys, "orig_argv", None)
        if reexec_argv:
            os.execvpe(reexec_argv[0], reexec_argv, os.environ)
        os.execvpe(sys.executable, [sys.executable, *sys.argv], os.environ)

    os.environ["_LIBPOSTAL_BOOTSTRAPPED"] = "1"


_bootstrap_libpostal_library_path()

from postal.parser import parse_address
from nominatim_helpers.zip_reapir import repair_zip_ri_ma
from nominatim_helpers.rapidfuzz_scorer import smart_score
from nominatim_helpers.nominatim_result_check import nominatim_result_check, SimpleCfg
from expand_abbreviations_in_road import expand_abbreviations_in_road
from uszipcode import SearchEngine


class NominatimSearch:
    _lookup_lock = threading.RLock()
    _bad_address_lookup_map: dict[str, str] | None = None
    _address_cache_maps: dict[str, dict[str, dict[str, str]]] = {}

    def __init__(
        self,
        base_url: str = "http://localhost:8080/search",
        timeout: int = 5,
        user_agent: str = "nominatim_search/1.0 (local)",
        parser_backend: str = "libpostal",
        postcode_search_limit: int = 50,
        fuzzy_threshold: int = 80,
        db_host: str | None = None,
        db_port: int | None = None,
        db_name: str | None = None,
        db_user: str | None = None,
        db_pass: str | None = None,
        db_country_code: str | None = None,
        db_radius_m: int | None = None,
        db_connect_timeout: int | None = None,
        db_statement_timeout_ms: int | None = None,
        use_address_cache: bool = True,
        save_address_cache: bool = True,
        address_cache_path: str | None = None,
        address_cache_data: dict[str, dict[str, str]] | None = None,
        address_cache_lock: threading.RLock | None = None,
    ) -> None:
        
        self.parser_backend = parser_backend
        self.postcode_search_limit = postcode_search_limit
        self.fuzzy_threshold = fuzzy_threshold

        # Nominatim Postgres DB connection settings
        # db_connect_timeout:
        # How long psycopg waits to open a Postgres connection (network + auth).
        # If exceeded, DB connection fails before any SQL runs.

        # db_statement_timeout_ms:
        # This runs SET statement_timeout = ... in Postgres
        # The server cancels any SQL statement that runs too long.
        # It applies to statements on that connection in _postcode_candidates.

        # timeout:
        # This is the HTTP timeout for requests.get(...) to Nominatim’s /search endpoint.

        self.base_url = base_url
        self.timeout = int(timeout)
        self.user_agent = user_agent
        self.db_host = db_host or os.getenv("NOM_DB_HOST", "localhost")
        self.db_port = int(db_port or os.getenv("NOM_DB_PORT", "5433"))
        self.db_name = db_name or os.getenv("NOM_DB_NAME", "nominatim")
        self.db_user = db_user or os.getenv("NOM_DB_USER", "nominatim")
        self.db_pass = db_pass or os.getenv("NOM_DB_PASS", "qaIACxO6wMR3")
        self.db_country_code = (db_country_code or os.getenv("NOM_DB_COUNTRY", "us")).lower()
        self.db_radius_m = int(db_radius_m or os.getenv("NOM_DB_RADIUS_M", "5000"))
        self.db_connect_timeout = (
            int(db_connect_timeout) if db_connect_timeout is not None else 5
        )
        self.db_statement_timeout_ms = (
            int(db_statement_timeout_ms) if db_statement_timeout_ms is not None else 8000
        )
        self.use_address_cache = bool(use_address_cache)
        self.save_address_cache = bool(save_address_cache)
        self.address_cache_data = address_cache_data
        self.address_cache_lock = address_cache_lock
        if address_cache_path:
            self.address_cache_path = os.path.abspath(address_cache_path)
        else:
            self.address_cache_path = os.path.abspath(os.path.join(
                os.path.dirname(__file__), "geocode_address_cache.csv"
            ))
        self.reset()

    def reset(self) -> None:
        self.raw_address: str = ""
        self.usaddress_tags: Optional[Dict[str, str]] = None
        self.query: str = ""
        self.latitude: str | None = None
        self.longitude: str | None = None
        self.nominatim_address: str = ""
        self.method: str = ""
        self.method_outputs: str = ""
        self.error: str = ""
        self.response: Any = None
        self.result_metadata: Dict[str, Any] = {}
        self.log: list[str] = []
        self._postcode_lookup_error: str = ""
        self._last_fuzzy_top_score: float | None = None
        self._cache_entry_missing_metadata: bool = False
        self.tag_metadata: Dict[str, Any] = {}
        self.search_metadata: Dict[str, Any] = {}
        self.process_metadata: Dict[str, Any] = {}

    def _log(self, message: str) -> None:
        self.log.append(message)

    def _refresh_process_metadata(self) -> None:
        combined: Dict[str, Any] = {}
        combined.update(self.tag_metadata)
        combined.update(self.search_metadata)
        combined["tag_metadata"] = self.tag_metadata
        combined["search_metadata"] = self.search_metadata
        self.process_metadata = combined

    @staticmethod
    def _normalize_cache_key(value: str) -> str:
        if value is None:
            return ""
        normalized = str(value).strip().casefold()
        normalized = " ".join(normalized.split())
        return normalized.strip(" ,")

    @staticmethod
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

    @classmethod
    def _load_bad_address_lookup_map(cls) -> dict[str, str]:
        with cls._lookup_lock:
            if cls._bad_address_lookup_map is not None:
                return cls._bad_address_lookup_map

            lookup_path = os.path.join(
                os.path.dirname(__file__), "nominatim_search_bad_address_lookup.csv"
            )
            legacy_path = os.path.join(
                os.path.dirname(__file__), "nominatim_search_bad_address_cache.csv"
            )
            source_path = lookup_path if os.path.exists(lookup_path) else legacy_path

            lookup: dict[str, str] = {}
            if os.path.exists(source_path):
                with open(source_path, newline="", encoding="utf-8") as handle:
                    reader = csv.DictReader(handle)
                    for row in reader:
                        raw = (row.get("address_raw") or "").strip()
                        address_update = (row.get("address_update") or "").strip()
                        if not raw or not address_update:
                            continue
                        lookup[cls._normalize_cache_key(raw)] = address_update

            cls._bad_address_lookup_map = lookup
            return lookup

    @classmethod
    def _load_address_cache_map(cls, cache_path: str) -> dict[str, dict[str, str]]:
        with cls._lookup_lock:
            if cache_path in cls._address_cache_maps:
                return cls._address_cache_maps[cache_path]

            lookup: dict[str, dict[str, str]] = {}
            if os.path.exists(cache_path):
                with open(cache_path, newline="", encoding="utf-8") as handle:
                    reader = csv.DictReader(handle)
                    for row in reader:
                        row_clean = {
                            str(k): (v.strip() if isinstance(v, str) else "")
                            for k, v in row.items()
                            if k is not None
                        }
                        raw = row_clean.get("address_raw", "")
                        if not raw:
                            continue
                        lookup[cls._normalize_cache_key(raw)] = row_clean

            cls._address_cache_maps[cache_path] = lookup
            return lookup

    def _lookup_bad_address(self, raw_address: str) -> str | None:
        lookup = self._load_bad_address_lookup_map()
        return lookup.get(self._normalize_cache_key(raw_address))

    def _lookup_address_cache(self, raw_address: str) -> dict[str, str] | None:
        if self.address_cache_data is not None:
            key = self._normalize_cache_key(raw_address)
            if self.address_cache_lock is not None:
                with self.address_cache_lock:
                    return (
                        self.address_cache_data.get(key)
                        or self.address_cache_data.get(raw_address)
                    )
            return self.address_cache_data.get(key) or self.address_cache_data.get(raw_address)
        lookup = self._load_address_cache_map(self.address_cache_path)
        return lookup.get(self._normalize_cache_key(raw_address))

    def _save_address_cache_entry(self) -> None:
        if not self.raw_address:
            return
        self._refresh_process_metadata()

        row = {
            "address_raw": self.raw_address,
            "address_geocode": self.query or self.raw_address,
            "address_nominatim": self.nominatim_address or "",
            "latitude": self.latitude or "",
            "longitude": self.longitude or "",
            "method": self.method or "",
            "error": self.error or "",
            "result_metadata": json.dumps(self.result_metadata or {}, sort_keys=True),
            "tag_metadata": json.dumps(self.tag_metadata or {}, sort_keys=True),
            "search_metadata": json.dumps(self.search_metadata or {}, sort_keys=True),
            "process_metadata": json.dumps(self.process_metadata or {}, sort_keys=True),
        }

        if self.address_cache_data is not None:
            key = self._normalize_cache_key(self.raw_address)
            if self.address_cache_lock is not None:
                with self.address_cache_lock:
                    self.address_cache_data[key] = row
            else:
                self.address_cache_data[key] = row
            return

        fieldnames = [
            "address_raw",
            "address_geocode",
            "address_nominatim",
            "latitude",
            "longitude",
            "method",
            "error",
            "result_metadata",
            "tag_metadata",
            "search_metadata",
            "process_metadata",
        ]

        with self._lookup_lock:
            cache_map = self._load_address_cache_map(self.address_cache_path)
            cache_map[self._normalize_cache_key(self.raw_address)] = row

            cache_dir = os.path.dirname(self.address_cache_path)
            if cache_dir:
                os.makedirs(cache_dir, exist_ok=True)
            with open(self.address_cache_path, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                for cache_row in cache_map.values():
                    writer.writerow({k: cache_row.get(k, "") for k in fieldnames})

    def _append_search_detail(self, search_detail: Dict[str, Any]) -> None:
        self.search_metadata.setdefault("search_details", []).append(search_detail)
        search_attempt = {
            "search_name": search_detail.get("search_name"),
            "attempted": search_detail.get("attempted"),
            "result_status": search_detail.get("result_status"),
            "error": search_detail.get("error"),
            "number_results": search_detail.get("number_results"),
            "result_check": search_detail.get("result_check"),
            "result_check_reason": search_detail.get("result_check_reason"),
            "result_check_logic": search_detail.get("result_check_logic"),
            "elapsed_ms": search_detail.get("elapsed_ms"),
            "query": search_detail.get("query"),
            "expected_zip": search_detail.get("expected_zip"),
            "expected_town": search_detail.get("expected_town"),
            "accepted_result_index": search_detail.get("accepted_result_index"),
        }
        self.search_metadata.setdefault("search_attempts", []).append(search_attempt)
        self._refresh_process_metadata()

    def _build_skipped_search_detail(
        self,
        search_name: str,
        reason: str,
        query: str = "",
        expected_zip: str = "",
        expected_town: str = "",
    ) -> Dict[str, Any]:
        return {
            "search_name": search_name,
            "attempted": False,
            "result_status": "skipped",
            "error": None,
            "number_results": 0,
            "result_check": None,
            "result_check_reason": reason,
            "result_check_logic": reason,
            "elapsed_ms": 0,
            "query": query,
            "expected_zip": expected_zip or None,
            "expected_town": expected_town or None,
            "accepted_result_index": None,
            "results": [],
        }

    def _apply_cached_result(self, cached_row: dict[str, str]) -> None:
        def _parse_dict(value: str) -> Dict[str, Any]:
            if not value:
                return {}
            try:
                parsed = json.loads(value)
            except Exception:
                return {}
            return parsed if isinstance(parsed, dict) else {}

        lat = (cached_row.get("latitude") or "").strip()
        lon = (cached_row.get("longitude") or "").strip()
        self.latitude = lat or None
        self.longitude = lon or None
        self.nominatim_address = cached_row.get("address_nominatim", "")
        self.query = cached_row.get("address_geocode", "") or self.raw_address
        self.method = cached_row.get("method", "")
        self.error = cached_row.get("error", "")
        self.response = None
        self.result_metadata = _parse_dict(cached_row.get("result_metadata", ""))
        cached_tag_metadata = _parse_dict(cached_row.get("tag_metadata", ""))
        cached_search_metadata = _parse_dict(cached_row.get("search_metadata", ""))
        cached_process_metadata = _parse_dict(cached_row.get("process_metadata", ""))

        if not cached_tag_metadata and isinstance(cached_process_metadata.get("tag_metadata"), dict):
            cached_tag_metadata = dict(cached_process_metadata.get("tag_metadata") or {})
        if not cached_search_metadata and isinstance(cached_process_metadata.get("search_metadata"), dict):
            cached_search_metadata = dict(cached_process_metadata.get("search_metadata") or {})
        if not cached_tag_metadata and cached_process_metadata:
            tag_keys = {
                "raw_address",
                "fix_zip_repair",
                "fix_state_abbreviation",
                "fix_state_abbreviation_before_tags",
                "fix_state_abbreviation_after_tags",
                "fix_expand_address_abbreviations_count",
                "fix_town_directional",
                "fix_address_number_non_numeric",
                "reverse_for_state_searched",
                "reverse_for_state_included",
                "reverse_for_state_number_results",
                "reverse_for_state_all_results_match",
                "revers_for_state_display_name",
                "address_tags",
                "address_tags_expanded",
                "missing_street_number",
                "missing_street_name",
                "missing_city",
                "missing_state",
                "missing_zip",
            }
            cached_tag_metadata = {
                key: cached_process_metadata.get(key)
                for key in tag_keys
                if key in cached_process_metadata
            }
            if (
                "fix_state_abbreviation_before_tag" in cached_process_metadata
                and "fix_state_abbreviation_before_tags" not in cached_tag_metadata
            ):
                cached_tag_metadata["fix_state_abbreviation_before_tags"] = (
                    cached_process_metadata.get("fix_state_abbreviation_before_tag")
                )
            if (
                "fix_state_abbreviation_after_tag" in cached_process_metadata
                and "fix_state_abbreviation_after_tags" not in cached_tag_metadata
            ):
                cached_tag_metadata["fix_state_abbreviation_after_tags"] = (
                    cached_process_metadata.get("fix_state_abbreviation_after_tag")
                )
        if not cached_search_metadata and cached_process_metadata:
            search_keys = {
                "raw_address",
                "bad_address_lookup_used",
                "address_cache_used",
                "search_attempts",
                "search_details",
                "search_method_accepted",
                "street_match_in_zip_attempted",
                "street_match_in_zip_number_candidates",
                "street_match_in_zip_top_score",
                "street_match_in_zip_top_accepted",
                "street_match_in_zip_elapsed_ms",
                "tiger_extrapolate_snap",
                "search_successful",
                "final_error",
                "elapsed_ms",
                "nominatim_results_returned_total",
                "nominatim_results_returned_by_method",
                "nominatim_results_returned_by_search",
            }
            cached_search_metadata = {
                key: cached_process_metadata.get(key)
                for key in search_keys
                if key in cached_process_metadata
            }
        if not cached_search_metadata and cached_process_metadata:
            cached_search_metadata = dict(cached_process_metadata)

        self.tag_metadata = cached_tag_metadata
        self.search_metadata = cached_search_metadata
        self.search_metadata["address_cache_used"] = True
        self._cache_entry_missing_metadata = not (
            self.result_metadata and self.tag_metadata and self.search_metadata
        )
        if cached_process_metadata:
            self.search_metadata["cached_process_metadata"] = cached_process_metadata
        self._refresh_process_metadata()
        self._log(
            "Address cache hit: "
            f"method={self.method!r} lat={self.latitude!r} lon={self.longitude!r}"
        )

    def _parse_address(self, raw_address: str) -> None:
        self.tag_metadata["fix_zip_repair"] = False
        self.tag_metadata["fix_state_abbreviation"] = False
        self.tag_metadata["fix_town_directional"] = False

        state_correction = {
            "rhode island": "RI",
            "r.i.": "RI",
            "massachusetts": "MA",
            "mass.": "MA",
            "m.a.": "MA",}
        
        for key, value in state_correction.items():
            if key in raw_address.lower():
                index = raw_address.lower().index(key)
                raw_address = raw_address[:index] + value + raw_address[index+len(key):]
                self._log(f"Corrected To 2 Letter State: {key!r} -> {value!r}")
                self.tag_metadata["fix_state_abbreviation"] = True

        town_corrections = {
            "n scituate": "North Scituate",
            "n. scituate": "North Scituate",
            "n kingstown": "North Kingstown",
            "s kingstown": "South Kingstown",
            "s. kingstown": "South Kingstown",
            "n providence": "North Providence",
            "n. providence": "North Providence",
            "n attleboro": "North Attleboro",
            "n. attleboro": "North Attleboro",
        }

        for key, value in town_corrections.items():
            if key in raw_address.lower():
                index = raw_address.lower().index(key)
                raw_address = raw_address[:index] + value + raw_address[index+len(key):]
                self._log(f"Corrected To Full Town Name: {key!r} -> {value!r}")
                self.tag_metadata["fix_town_directional"] = True

        zip_repair_obj = repair_zip_ri_ma(raw_address)
        address_zip_repaired = zip_repair_obj.cleaned_address
        zip5 = zip_repair_obj.zip5
        self.tag_metadata["fix_zip_repair"] = zip_repair_obj.zip_source in {
            "zip4_trailing",
            "zip4_after_state",
        }
        if zip5:
            self._log(
                "Zip repair applied: "
                f"source={zip_repair_obj.zip_source!r} zip5={zip5!r} "
                f"cleaned_address={address_zip_repaired!r}"
            )

        self.address_tags_raw = {}
        self.address_type = ""
        self.address_tags_expanded = {}

        try:
            self.address_tags_raw, self.address_type = usaddress.tag(address_zip_repaired)
            self._log("usaddress tags raw: " + repr(self.address_tags_raw))
        except Exception as exc:
            self._log(f"usaddress tags: First attempt failed, will evaluate how to repair address and try again.")
            address_tags_raw = {}
            for value, label in exc.parsed_string:
                address_tags_raw[label] = value
            
            if "ZipCode" not in address_tags_raw:
                self._log("Failed usaddress tag() did not include ZipCode; skipping further processing.")
                return None
            if "StateName" in address_tags_raw:
                self._log("Failed usaddress tag() included StateName; skipping further processing.")
                return None

            zip_search = SearchEngine()
            zipcode_info = zip_search.by_zipcode(address_tags_raw["ZipCode"])
            state_abbr = zipcode_info.state_abbr

            if not state_abbr:
                self._log("Could not find state abbreviation for ZipCode tag; skipping state insertion.")
                return None

            # Insert State Abreviation into address_zip_repaired
            address_zip_repaired = address_zip_repaired.replace(address_tags_raw["ZipCode"], state_abbr + " " + address_tags_raw["ZipCode"])
            self._log(f"Inserted State Abbreviation into address: {state_abbr} -> {address_zip_repaired!r}")
            self.tag_metadata["fix_state_abbreviation_before_tags"] = True

            try:
                self.address_tags_raw, self.address_type = usaddress.tag(address_zip_repaired)
                self._log("usaddress tags raw: " + repr(self.address_tags_raw))
            except Exception as exc2:
                self._log(f"usaddress tag() failed again after state insertion: {exc2}")
                return None
        

        if "ZipCode" in self.address_tags_raw and "StateName" not in self.address_tags_raw:
            zip_search = SearchEngine()
            zipcode_info = zip_search.by_zipcode(self.address_tags_raw["ZipCode"])
            state_abbr = zipcode_info.state_abbr
            
            if not state_abbr:
                self._log("Could not find state abbreviation for ZipCode tag; skipping state insertion.")
                return None
            
            self.address_tags_raw["StateName"] = state_abbr
            self.tag_metadata["fix_state_abbreviation_after_tags"] = True
            self.tag_metadata["fix_state_abbreviation"] = True
            self._log(f"Added StateName tag based on ZipCode tag: {state_abbr!r} -> {self.address_tags_raw!r}")

        if "AddressNumber" in self.address_tags_raw:
            address_number = self.address_tags_raw["AddressNumber"]
            if not address_number.isdigit():
                number_numeric = "".join(c for c in address_number if c.isdigit())
                self.address_tags_raw["AddressNumber"] = number_numeric
                number_non_numeric = "".join(c for c in address_number if not c.isdigit())
                self.tag_metadata["fix_address_number_non_numeric"] = True
                if "OccupancyType" not in self.address_tags_raw:
                    self.address_tags_raw["OccupancyType"] = "Unit"
                    self.address_tags_raw["OccupancyIdentifier"] = number_non_numeric
                    self._log(f"AddressNumber had non-digit chars; moved to OccupancyType/Identifier: {number_non_numeric!r}")
                elif "SubaddressType" not in self.address_tags_raw:
                    self.address_tags_raw["SubaddressType"] = "Unit"
                    self.address_tags_raw["SubaddressIdentifier"] = number_non_numeric
                    self._log(f"AddressNumber had non-digit chars; moved to SubaddressType/Identifier: {number_non_numeric!r}")
                else:
                    self._log(f"AddressNumber had non-digit chars but OccupancyType and SubaddressType already exist; leaving as-is: {number_non_numeric!r}")

        self.address_tags_expanded, count = self.expand_address_abbreviations(self.address_tags_raw)
        self._log("usaddress tags expanded: " + repr(self.address_tags_expanded))
        self.tag_metadata["fix_expand_address_abbreviations_count"] = count

        # Try to infer only StateName when both StateName and ZipCode are missing.
        self._reverse_for_state()

    def expand_address_abbreviations(self, address_tags):
        # Standardize common abbreviations dictionary
        abbreviations = {
            'st': 'Street', 'st.': 'Street',
            'ave': 'Avenue', 'ave.': 'Avenue',
            'blvd': 'Boulevard', 'blvd.': 'Boulevard',
            'rd': 'Road', 'rd.': 'Road',
            'ct': 'Court', 'ct.': 'Court',
            'ln': 'Lane', 'ln.': 'Lane',
            'dr': 'Drive', 'dr.': 'Drive',
            'pl': 'Place', 'pl.': 'Place',
            'sq': 'Square', 'sq.': 'Square',
            'pkwy': 'Parkway', 'pkwy.': 'Parkway',
            'cir': 'Circle', 'cir.': 'Circle',
            'n': 'North', 's': 'South',
            'e': 'East', 'w': 'West',
            'apt': 'Apartment', 'apt.': 'Apartment',
            'ste': 'Suite', 'ste.': 'Suite'
        }
        count=0
        expanded_tags = {}
        for key, value in address_tags.items():
            # Only expand if it's a type or direction component
            if key in ['StreetNamePreType', 'StreetNamePostType', 
                            'StreetNamePreDirectional', 'StreetNamePostDirectional', 
                            'StreetNamePreModifier', 'StreetNamePostModifier',
                            'OccupancyType']:
                expanded_tags[key] = abbreviations.get(value.lower(), value)
                count+=1
            else:
                expanded_tags[key] = value
        return expanded_tags,count


    def _build_street_value(self) -> str:
        street_parts = [
            self.address_tags_expanded.get("StreetNamePreDirectional", ""),
            self.address_tags_expanded.get("StreetNamePreType", ""),
            self.address_tags_expanded.get("StreetName", ""),
            self.address_tags_expanded.get("StreetNamePostType", ""),
            self.address_tags_expanded.get("StreetNamePostDirectional", ""),
        ]
        return " ".join([p for p in street_parts if p]).strip()

    def _build_query(self,spec) -> list[str]:
        query_parts = []
        
        if "AddressNumber" in spec:
            address_number=self.address_tags_expanded.get("AddressNumber", "")
            if address_number:
                query_parts.append(address_number)

        if "StreetName" in spec:
            street = self._build_street_value()
            if street:
                query_parts.append(street)

        if "PlaceName" in spec:
            place_name = self.address_tags_expanded.get("PlaceName", "")
            if place_name:
                query_parts.append(place_name)

        if "StateName" in spec:
            state_name = self.address_tags_expanded.get("StateName", "")
            if state_name:
                query_parts.append(state_name)

        if "ZipCode" in spec:
            zip_code = self.address_tags_expanded.get("ZipCode", "")
            if zip_code:
                query_parts.append(zip_code)

        return query_parts

    def _reverse_for_state(self) -> None:
        self.tag_metadata["reverse_for_state_searched"] = False
        self.tag_metadata["reverse_for_state_included"] = False
        self.tag_metadata["reverse_for_state_number_results"] = 0
        self.tag_metadata["reverse_for_state_all_results_match"] = None
        self.tag_metadata["revers_for_state_display_name"] = None

        state_value = (self.address_tags_expanded.get("StateName") or "").strip()
        zip_value = (self.address_tags_expanded.get("ZipCode") or "").strip()
        if state_value or zip_value:
            self._log(
                "reverse_for_state skipped: "
                f"state_present={bool(state_value)}, zip_present={bool(zip_value)}"
            )
            return

        house_number = (self.address_tags_expanded.get("AddressNumber") or "").strip()
        street_value = self._build_street_value()
        city_value = (self.address_tags_expanded.get("PlaceName") or "").strip()
        if not house_number or not street_value or not city_value:
            self._log(
                "reverse_for_state skipped: missing required tags "
                f"AddressNumber={bool(house_number)}, StreetName={bool(street_value)}, "
                f"PlaceName={bool(city_value)}"
            )
            return

        query_text = ", ".join([house_number, street_value, city_value])
        params = {
            "q": query_text,
            "format": "json",
            "addressdetails": 1,
            "limit": 10,
            "countrycodes": "us",
        }
        self.tag_metadata["reverse_for_state_searched"] = True
        self._log(f"reverse_for_state query={query_text!r}")

        try:
            resp = requests.get(
                self.base_url,
                params=params,
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent},
            )
            resp.raise_for_status()
            data = resp.json()
        except requests_exceptions.Timeout:
            self._log("reverse_for_state timeout.")
            return
        except requests_exceptions.RequestException as exc:
            self._log(f"reverse_for_state request error: {exc}")
            return

        if not isinstance(data, list):
            data = []
        self.tag_metadata["reverse_for_state_number_results"] = len(data)

        state_results: list[tuple[str, str]] = []
        for res in data:
            address = res.get("address") or {}
            state_name = (address.get("state") or "").strip()
            if state_name:
                state_results.append((state_name, (res.get("display_name") or "").strip()))

        if not state_results:
            self.tag_metadata["reverse_for_state_all_results_match"] = None
            self._log("reverse_for_state: no result states found.")
            return

        normalized_states: dict[str, str] = {}
        for state_name, _ in state_results:
            key = state_name.casefold().strip()
            if key and key not in normalized_states:
                normalized_states[key] = state_name

        if len(normalized_states) == 1:
            inferred_state = next(iter(normalized_states.values()))
            display_name_value = None
            for state_name, display_name in state_results:
                if state_name.casefold().strip() == inferred_state.casefold().strip():
                    if display_name:
                        display_name_value = display_name
                        break
            self.address_tags_raw["StateName"] = inferred_state
            self.address_tags_expanded["StateName"] = inferred_state
            self.tag_metadata["reverse_for_state_included"] = True
            self.tag_metadata["reverse_for_state_all_results_match"] = True
            self.tag_metadata["revers_for_state_display_name"] = display_name_value
            self._log(f"reverse_for_state included StateName={inferred_state!r}")
            return

        self.tag_metadata["reverse_for_state_all_results_match"] = False
        self._log(
            "reverse_for_state ambiguous states: "
            + ", ".join(sorted(normalized_states.values()))
        )

    def _request(
        self,
        query: str,
        search_name: str,
        expected_zip: str = "",
        expected_town: str = "",
    ) -> tuple[bool, str, Dict[str, Any]]:
        """
        Executes a Nominatim query and validates the top result with
        nominatim_result_check (filters broad/area-like matches).
        """
        self.query = query
        self.method = search_name
        search_detail: Dict[str, Any] = {
            "search_name": search_name,
            "attempted": True,
            "result_status": "error",
            "error": None,
            "number_results": 0,
            "result_check": None,
            "result_check_reason": None,
            "result_check_logic": None,
            "elapsed_ms": 0,
            "query": query,
            "expected_zip": expected_zip or None,
            "expected_town": expected_town or None,
            "accepted_result_index": None,
            "results": [],
        }
        started_at = time.perf_counter()
        params = {
            "q": query,
            "format": "json",
            "addressdetails": 1,
            "limit": 10,
        }
        try:
            resp = requests.get(
                self.base_url,
                params=params,
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent},
            )
            resp.raise_for_status()
            data = resp.json()
            self.response = data
            returned_count = len(data) if data else 0
            self.search_metadata["nominatim_results_returned_total"] = (
                int(self.search_metadata.get("nominatim_results_returned_total", 0))
                + returned_count
            )
            returned_by_search = self.search_metadata.setdefault(
                "nominatim_results_returned_by_search", {}
            )
            returned_by_search[search_name] = int(
                returned_by_search.get(search_name, 0)
            ) + returned_count
            returned_by_method = self.search_metadata.setdefault(
                "nominatim_results_returned_by_method", {}
            )
            returned_by_method[search_name] = int(
                returned_by_method.get(search_name, 0)
            ) + returned_count

            if not data:
                self._log(f"Nominatim, search={search_name!r}, Returned empty result list.")
                self.error = "No results"
                search_detail["result_status"] = "none_found"
                search_detail["error"] = self.error
                search_detail["number_results"] = 0
                return False, self.error, search_detail
            
            search_detail["result_status"] = "returned"
            search_detail["number_results"] = len(data)
            self._log(
                f"Nominatim, search={search_name!r} query={query!r} response_count={len(data)}"
            )

            accept_result = False
            accepted_diag: Dict[str, Any] = {}
            accepted_res: Dict[str, Any] | None = None
            accepted_idx: int | None = None
            rejected_reason_texts: list[str] = []
            rejected_logic_texts: list[str] = []
            for idx, res in enumerate(data):
                (
                    accept_result,
                    rejection_reason,
                    rejection_logic,
                    diag,
                ) = nominatim_result_check(
                    res,
                    expected_zip=expected_zip,
                    expected_town=expected_town,
                )
                search_detail["results"].append(
                    {
                        "result_index": idx,
                        "display_name": res.get("display_name"),
                        "class": res.get("class"),
                        "type": res.get("type"),
                        "place_rank": res.get("place_rank"),
                        "accepted": bool(accept_result),
                        "rejection_reason": rejection_reason,
                        "rejection_logic": rejection_logic,
                    }
                )

                if accept_result:
                    self._log(
                        f"Result Accepted: search={search_name!r} diag={diag}"
                    )
                    accepted_diag = diag
                    accepted_res = res
                    accepted_idx = idx
                    break

                reason_text = rejection_reason or "rejected"
                logic_text = rejection_logic or reason_text
                rejected_reason_texts.append(reason_text)
                rejected_logic_texts.append(logic_text)
                self.error = f"Rejected result: {reason_text}"
                search_detail["result_check"] = "rejected"
                search_detail["result_check_reason"] = reason_text
                search_detail["result_check_logic"] = logic_text
                self._log(
                    "Result Rejected: "
                    f"display_name={res.get('display_name')!r}, "
                    f"reason={reason_text!r}, logic={logic_text!r}, diag={diag}"
                )
            
            if not accept_result:
                self._log("No acceptable results found.")
                self.error = "No acceptable results"
                if rejected_reason_texts:
                    self.error = (
                        "No acceptable results; rejected_reasons="
                        + " | ".join(rejected_reason_texts)
                    )
                search_detail["error"] = self.error
                search_detail["result_check"] = "rejected"
                search_detail["result_check_reason"] = (
                    " | ".join(rejected_reason_texts) if rejected_reason_texts else None
                )
                search_detail["result_check_logic"] = (
                    " | ".join(rejected_logic_texts) if rejected_logic_texts else None
                )
                return False, self.error, search_detail

            search_detail["result_check"] = "accepted"
            search_detail["result_check_reason"] = None
            search_detail["result_check_logic"] = None
            search_detail["accepted_result_index"] = accepted_idx
            res = accepted_res or {}
            addr = res.get("address") or {}
            self.result_metadata = {
                "search_name": search_name,
                "search_query": query,
                "search_number_results": returned_count,
                "accepted_result_index": accepted_idx,
                "osm_type": res.get("osm_type"),
                "osm_id": res.get("osm_id"),
                "place_id": res.get("place_id"),
                "lat": res.get("lat"),
                "lon": res.get("lon"),
                "place_rank": res.get("place_rank"),
                "class": res.get("class"),
                "type": res.get("type"),
                "addresstype": res.get("addresstype"),
                "importance": res.get("importance"),
                "bbox_max_dim_m": accepted_diag.get("bbox_max_dim_m"),
                "display_name": res.get("display_name"),
                "addr_house_number": addr.get("house_number"),
                "addr_road": addr.get("road"),
                "addr_postcode": addr.get("postcode"),
                "addr_city": addr.get("city") or addr.get("town") or addr.get("village"),
                "addr_state": addr.get("state"),
                "checker_place_rank": accepted_diag.get("place_rank"),
                "checker_expected_zip5": accepted_diag.get("expected_zip5"),
                "checker_result_zip5": accepted_diag.get("result_zip5"),
                "checker_zip_match": accepted_diag.get("zip_match"),
                "checker_expected_town": accepted_diag.get("expected_town"),
                "checker_expected_town_normalized": accepted_diag.get("expected_town_normalized"),
                "checker_town_match": accepted_diag.get("town_match"),
                "checker_town_match_keys": accepted_diag.get("town_match_keys"),
                "checker_reasons": accepted_diag.get("reasons"),
            }
            self._log(f"Accepted result metadata: {self.result_metadata}")

            self.latitude = res.get("lat")
            self.longitude = res.get("lon")
            self.nominatim_address = res.get("display_name", "")
            self.error = ""
            self._log(
                f"Success: lat={self.latitude}, lon={self.longitude}, "
                f"display_name={self.nominatim_address!r}"
            )
            return True, "", search_detail

        except requests_exceptions.Timeout:
            self.error = "Timeout"
            search_detail["result_status"] = "error"
            search_detail["error"] = self.error
            self._log("Request timeout.")
            return False, self.error, search_detail
        except requests_exceptions.RequestException as exc:
            self.error = str(exc)
            search_detail["result_status"] = "error"
            search_detail["error"] = self.error
            self._log(f"Request error: {self.error}")
            return False, self.error, search_detail
        finally:
            search_detail["elapsed_ms"] = int((time.perf_counter() - started_at) * 1000)

    def _find_postcode_geom_column(self, conn) -> str:
        sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name='location_postcode'
              AND column_name IN ('centroid','geometry')
            ORDER BY CASE column_name WHEN 'centroid' THEN 1 ELSE 2 END
            LIMIT 1;
        """
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if not row:
                raise RuntimeError(
                    "location_postcode has neither centroid nor geometry column."
                )
            return row[0]

    def _find_column_operator(self, conn, table: str, column: str) -> tuple[str, str]:
        """
        Return the correct operator and cast for a column that may be hstore or json.
        - hstore: use '->' and cast to ::text
        - json/jsonb: use '->>' with no cast
        """
        sql = """
            SELECT data_type, udt_name
            FROM information_schema.columns
            WHERE table_name=%s
              AND column_name=%s
            LIMIT 1;
        """
        with conn.cursor() as cur:
            cur.execute(sql, (table, column))
            row = cur.fetchone()
            if not row:
                return "->>", ""
            data_type, udt_name = row[0], row[1]
            if udt_name == "hstore":
                return "->", "::text"
            if data_type in ("json", "jsonb"):
                return "->>", ""
        return "->>", ""

    def _postcode_candidates(self, postcode: str) -> list[str]:
        if not postcode:
            return []
        if psycopg is None:
            self._log("psycopg is not available; skipping postcode DB lookup.")
            self._postcode_lookup_error = "db_unavailable"
            return []

        dsn = (
            f"host={self.db_host} port={self.db_port} dbname={self.db_name} "
            f"user={self.db_user} password={self.db_pass}"
        )
        try:
            with psycopg.connect(dsn, connect_timeout=self.db_connect_timeout) as conn:
                with conn.cursor() as cur:
                    timeout_ms = int(self.db_statement_timeout_ms)
                    cur.execute(f"SET statement_timeout = {timeout_ms};")
                geom_col = self._find_postcode_geom_column(conn)
                addr_op, addr_cast = self._find_column_operator(conn, "placex", "address")
                name_op, name_cast = self._find_column_operator(conn, "placex", "name")
                tiger_sql = f"""
                WITH roads AS (
                  SELECT DISTINCT NULLIF(BTRIM(p.name{name_op}'name'{name_cast}), '') AS road_name
                  FROM location_property_tiger t
                  JOIN placex p ON p.place_id = t.parent_place_id
                  WHERE t.postcode = %s
                  UNION
                  SELECT DISTINCT NULLIF(BTRIM(p.name{name_op}'name:en'{name_cast}), '') AS road_name
                  FROM location_property_tiger t
                  JOIN placex p ON p.place_id = t.parent_place_id
                  WHERE t.postcode = %s
                  UNION
                  SELECT DISTINCT NULLIF(BTRIM(p.name{name_op}'alt_name'{name_cast}), '') AS road_name
                  FROM location_property_tiger t
                  JOIN placex p ON p.place_id = t.parent_place_id
                  WHERE t.postcode = %s
                  UNION
                  SELECT DISTINCT NULLIF(BTRIM(p.name{name_op}'official_name'{name_cast}), '') AS road_name
                  FROM location_property_tiger t
                  JOIN placex p ON p.place_id = t.parent_place_id
                  WHERE t.postcode = %s
                  UNION
                  SELECT DISTINCT NULLIF(BTRIM(p.address{addr_op}'road'{addr_cast}), '') AS road_name
                  FROM location_property_tiger t
                  JOIN placex p ON p.place_id = t.parent_place_id
                  WHERE t.postcode = %s
                )
                SELECT road_name
                FROM roads
                WHERE road_name IS NOT NULL
                ORDER BY road_name;
                """
                with conn.cursor() as cur:
                    cur.execute(tiger_sql, (postcode, postcode, postcode, postcode, postcode))
                    rows = cur.fetchall()
                candidates = [r[0] for r in rows if r and r[0]]
                if not candidates:
                    self._log("Tiger postcode search returned no candidates; falling back to geometry lookup.")
                    sql = f"""
                    WITH z AS (
                    SELECT {geom_col}::geometry AS g
                    FROM location_postcode
                    WHERE country_code = %s AND postcode = %s
                    LIMIT 1
                    ),
                    roads AS (
                    SELECT DISTINCT
                        NULLIF(
                        BTRIM(
                            COALESCE(
                            p.name{name_op}'name'{name_cast},
                            p.name{name_op}'name:en'{name_cast},
                            p.name{name_op}'alt_name'{name_cast},
                            p.name{name_op}'official_name'{name_cast},
                            p.address{addr_op}'road'{addr_cast},
                            p.address{addr_op}'pedestrian'{addr_cast},
                            p.address{addr_op}'footway'{addr_cast},
                            p.address{addr_op}'path'{addr_cast}
                            )
                        ),
                        ''
                        ) AS road_name
                    FROM placex p, z
                    WHERE p.class = 'highway'
                        AND p.geometry IS NOT NULL
                        AND ST_DWithin(p.geometry::geography, z.g::geography, %s)
                    )
                    SELECT road_name
                    FROM roads
                    WHERE road_name IS NOT NULL
                    ORDER BY road_name;
                    """
                    cur.execute(
                        sql, (self.db_country_code, postcode, self.db_radius_m)
                    )
                    rows = cur.fetchall()
                    candidates = [r[0] for r in rows if r and r[0]]
            unique = sorted(set(candidates))
            self._log(f"Postcode DB candidates found: {len(unique)}")
            #self._log(f"Candidates: {unique!r}")
            return unique
        except Exception as exc:
            msg = str(exc)
            if "statement timeout" in msg.lower() or "timeout" in msg.lower():
                self._postcode_lookup_error = "db_timeout"
            else:
                self._postcode_lookup_error = "db_error"
            self._log(f"Postcode DB lookup error: {exc}")
            return []

    def _fuzzy_match_road(self, target_road: str, candidates: list[str]) -> tuple[str, int] | None:
        self._last_fuzzy_top_score = None
        if not target_road or not candidates:
            return None
        if rf_process is None or rf_fuzz is None:
            self._log("rapidfuzz is not available.")
            return None
        processor = rf_utils.default_process if rf_utils is not None else None
        target_expanded = expand_abbreviations_in_road(target_road)
        candidates_expanded = [expand_abbreviations_in_road(c) for c in candidates]
        self._log(f"Fuzzy target road: {target_road!r}")
        self._log(f"Fuzzy target expanded: {target_expanded!r}")

        choice_map = {idx: name for idx, name in enumerate(candidates_expanded)}
        top_matches = rf_process.extract(
            target_expanded,
            choice_map,
            scorer=smart_score,
            processor=processor,
            limit=5,
        )
        top_pretty = [
            (candidates[m[2]], int(m[1])) for m in top_matches if m is not None
        ]
        self._log(f"Top road matches: {top_pretty!r}")
        match = rf_process.extractOne(
            target_expanded,
            choice_map,
            scorer=smart_score,
            processor=processor,
        )
        if not match:
            return None
        _, score, match_idx = match
        self._last_fuzzy_top_score = float(score)
        match_name = candidates[match_idx]
        self._log(f"Best road match: {match_name!r} score={score}")
        if score < self.fuzzy_threshold:
            return None
        return match_name, int(score)

    @staticmethod
    def _parse_house_number_int(value: Any) -> int | None:
        if value is None:
            return None
        match = re.search(r"\d+", str(value))
        if not match:
            return None
        return int(match.group(0))

    @staticmethod
    def _lerp(start: float, end: float, fraction: float) -> float:
        return start + (end - start) * fraction

    def _search_tiger_extrapolate_snap(
        self,
        zip_code: str,
        fuzzy_street_name: str,
        address_number: str,
        expected_town: str = "",
    ) -> tuple[bool, str, Dict[str, Any]]:
        search_name = "tiger_extrapolate_snap"
        started_at = time.perf_counter()
        query_text = (
            f"postcode={zip_code}; street_like={fuzzy_street_name}; "
            f"address_number={address_number}"
        )
        tiger_meta = self.search_metadata.setdefault(
            "tiger_extrapolate_snap",
            {
                "attempted": False,
                "outcome": "not_attempted",
                "mode": "not_attempted",
                "rows_returned": 0,
                "elapsed_ms": 0,
                "error": None,
            },
        )
        tiger_meta["attempted"] = True
        tiger_meta["outcome"] = "unsuccessful"
        tiger_meta["mode"] = "unsuccessful"
        tiger_meta["rows_returned"] = 0
        tiger_meta["error"] = None

        search_detail: Dict[str, Any] = {
            "search_name": search_name,
            "attempted": True,
            "result_status": "error",
            "error": None,
            "number_results": 0,
            "result_check": None,
            "result_check_reason": None,
            "result_check_logic": None,
            "elapsed_ms": 0,
            "query": query_text,
            "expected_zip": zip_code or None,
            "expected_town": expected_town or None,
            "accepted_result_index": None,
            "results": [],
        }

        def _finalize(
            ok: bool,
            error_text: str,
        ) -> tuple[bool, str, Dict[str, Any]]:
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            search_detail["elapsed_ms"] = elapsed_ms
            tiger_meta["elapsed_ms"] = elapsed_ms
            return ok, error_text, search_detail

        house_num = self._parse_house_number_int(address_number)
        if not zip_code or not fuzzy_street_name or house_num is None:
            reason = "missing_required_inputs:zip_or_street_or_address_number"
            self.error = reason
            search_detail["result_status"] = "skipped"
            search_detail["error"] = reason
            search_detail["result_check"] = "rejected"
            search_detail["result_check_reason"] = reason
            search_detail["result_check_logic"] = (
                f"zip_code={zip_code!r}, fuzzy_street_name={fuzzy_street_name!r}, "
                f"address_number={address_number!r}, parsed_house_num={house_num!r}"
            )
            tiger_meta["error"] = reason
            return _finalize(False, self.error)

        if psycopg is None:
            self.error = "db_unavailable"
            search_detail["result_status"] = "error"
            search_detail["error"] = self.error
            tiger_meta["error"] = self.error
            return _finalize(False, self.error)

        dsn = (
            f"host={self.db_host} port={self.db_port} dbname={self.db_name} "
            f"user={self.db_user} password={self.db_pass}"
        )
        sql = """
            SELECT
              t.place_id,
              t.parent_place_id,
              t.postcode,
              t.startnumber::text AS startnumber_text,
              t.endnumber::text   AS endnumber_text,
              t.step::text        AS step_text,
              p.name::text AS road_name_text,
              p.class AS road_class,
              p.type  AS road_type,
              ST_X(ST_StartPoint(t.linegeo::geometry)) AS start_lon,
              ST_Y(ST_StartPoint(t.linegeo::geometry)) AS start_lat,
              ST_X(ST_EndPoint(t.linegeo::geometry))   AS end_lon,
              ST_Y(ST_EndPoint(t.linegeo::geometry))   AS end_lat
            FROM location_property_tiger t
            JOIN placex p ON p.place_id = t.parent_place_id
            WHERE t.postcode = %s
              AND p.name::text ILIKE '%%' || %s || '%%'
            ORDER BY
              p.name::text,
              LEAST(t.startnumber, t.endnumber),
              GREATEST(t.startnumber, t.endnumber);
        """

        try:
            with psycopg.connect(dsn, connect_timeout=self.db_connect_timeout) as conn:
                with conn.cursor() as cur:
                    timeout_ms = int(self.db_statement_timeout_ms)
                    cur.execute(f"SET statement_timeout = {timeout_ms};")
                    cur.execute(sql, (zip_code, fuzzy_street_name))
                    db_rows = cur.fetchall()
                    col_names = [desc.name for desc in cur.description]
        except Exception as exc:
            self.error = f"tiger_query_error:{exc}"
            search_detail["result_status"] = "error"
            search_detail["error"] = self.error
            tiger_meta["error"] = self.error
            return _finalize(False, self.error)

        tiger_rows: list[dict[str, Any]] = [
            {col_names[i]: row[i] for i in range(len(col_names))}
            for row in db_rows
        ]
        search_detail["result_status"] = "returned"
        search_detail["number_results"] = len(tiger_rows)
        tiger_meta["rows_returned"] = len(tiger_rows)
        if not tiger_rows:
            self.error = "No TIGER rows returned"
            search_detail["result_status"] = "none_found"
            search_detail["error"] = self.error
            search_detail["result_check"] = "rejected"
            search_detail["result_check_reason"] = "no_tiger_rows"
            search_detail["result_check_logic"] = (
                f"zip_code={zip_code!r}, fuzzy_street_name={fuzzy_street_name!r}"
            )
            tiger_meta["error"] = self.error
            return _finalize(False, self.error)

        valid_rows: list[dict[str, Any]] = []
        for idx, row in enumerate(tiger_rows):
            start_num = self._parse_house_number_int(row.get("startnumber_text"))
            end_num = self._parse_house_number_int(row.get("endnumber_text"))
            step_num = self._parse_house_number_int(row.get("step_text"))
            start_lat = row.get("start_lat")
            start_lon = row.get("start_lon")
            end_lat = row.get("end_lat")
            end_lon = row.get("end_lon")

            result_row = {
                "result_index": idx,
                "display_name": row.get("road_name_text"),
                "class": row.get("road_class"),
                "type": row.get("road_type"),
                "place_rank": None,
                "accepted": False,
                "rejection_reason": "not_selected",
                "rejection_logic": "",
                "startnumber": row.get("startnumber_text"),
                "endnumber": row.get("endnumber_text"),
                "step": row.get("step_text"),
            }

            if (
                start_num is None
                or end_num is None
                or start_lat is None
                or start_lon is None
                or end_lat is None
                or end_lon is None
            ):
                result_row["rejection_reason"] = "invalid_tiger_row"
                result_row["rejection_logic"] = (
                    f"start_num={start_num!r}, end_num={end_num!r}, "
                    f"start_lat={start_lat!r}, start_lon={start_lon!r}, "
                    f"end_lat={end_lat!r}, end_lon={end_lon!r}"
                )
                search_detail["results"].append(result_row)
                continue

            low_num = min(start_num, end_num)
            high_num = max(start_num, end_num)
            if start_num <= end_num:
                low_lat, low_lon = float(start_lat), float(start_lon)
                high_lat, high_lon = float(end_lat), float(end_lon)
            else:
                low_lat, low_lon = float(end_lat), float(end_lon)
                high_lat, high_lon = float(start_lat), float(start_lon)

            parity_ok = True
            if step_num == 2:
                parity_ok = (house_num % 2) == (start_num % 2)
            range_span = high_num - low_num
            midpoint = (low_num + high_num) / 2.0

            result_row["rejection_logic"] = (
                f"range=[{low_num},{high_num}], step={step_num}, parity_ok={parity_ok}"
            )
            search_detail["results"].append(result_row)
            valid_rows.append(
                {
                    "index": idx,
                    "raw": row,
                    "start_num": start_num,
                    "end_num": end_num,
                    "step_num": step_num,
                    "low_num": low_num,
                    "high_num": high_num,
                    "low_lat": low_lat,
                    "low_lon": low_lon,
                    "high_lat": high_lat,
                    "high_lon": high_lon,
                    "parity_ok": parity_ok,
                    "range_span": range_span,
                    "midpoint": midpoint,
                }
            )

        if not valid_rows:
            self.error = "No usable TIGER rows"
            search_detail["error"] = self.error
            search_detail["result_check"] = "rejected"
            search_detail["result_check_reason"] = "no_usable_tiger_rows"
            search_detail["result_check_logic"] = "all returned rows missing numeric range or geometry"
            tiger_meta["error"] = self.error
            return _finalize(False, self.error)

        selected_indices: list[int] = []
        selected_mode = "unsuccessful"
        selected_logic = ""
        selected_lat: float | None = None
        selected_lon: float | None = None
        selected_road_name = fuzzy_street_name
        selected_road_class: str | None = None
        selected_road_type: str | None = None
        selected_place_id: Any = None

        inside_rows = [
            r for r in valid_rows
            if r["parity_ok"] and r["low_num"] <= house_num <= r["high_num"]
        ]
        if inside_rows:
            best = min(
                inside_rows,
                key=lambda r: (r["range_span"], abs(house_num - r["midpoint"]))
            )
            frac = 0.5
            if best["range_span"] > 0:
                frac = (house_num - best["low_num"]) / best["range_span"]
            frac = max(0.0, min(1.0, frac))
            selected_lat = self._lerp(best["low_lat"], best["high_lat"], frac)
            selected_lon = self._lerp(best["low_lon"], best["high_lon"], frac)
            selected_indices = [best["index"]]
            selected_mode = "extrapolated"
            selected_logic = (
                "within_range_interpolation: "
                f"house_num={house_num}, range=[{best['low_num']},{best['high_num']}], frac={frac:.6f}"
            )
            selected_road_name = str(best["raw"].get("road_name_text") or fuzzy_street_name)
            selected_road_class = best["raw"].get("road_class")
            selected_road_type = best["raw"].get("road_type")
            selected_place_id = best["raw"].get("place_id")
        else:
            sorted_rows = sorted(valid_rows, key=lambda r: (r["low_num"], r["high_num"]))
            between_choice: tuple[dict[str, Any], dict[str, Any]] | None = None
            for left, right in zip(sorted_rows, sorted_rows[1:]):
                if not left["parity_ok"] or not right["parity_ok"]:
                    continue
                if left["high_num"] < house_num < right["low_num"]:
                    if between_choice is None:
                        between_choice = (left, right)
                        continue
                    current_gap = right["low_num"] - left["high_num"]
                    best_gap = between_choice[1]["low_num"] - between_choice[0]["high_num"]
                    if current_gap < best_gap:
                        between_choice = (left, right)

            if between_choice is not None:
                left, right = between_choice
                gap = right["low_num"] - left["high_num"]
                frac = 0.5
                if gap > 0:
                    frac = (house_num - left["high_num"]) / gap
                frac = max(0.0, min(1.0, frac))
                selected_lat = self._lerp(left["high_lat"], right["low_lat"], frac)
                selected_lon = self._lerp(left["high_lon"], right["low_lon"], frac)
                selected_indices = [left["index"], right["index"]]
                selected_mode = "extrapolated"
                selected_logic = (
                    "between_ranges_extrapolation: "
                    f"house_num={house_num}, lower_high={left['high_num']}, "
                    f"upper_low={right['low_num']}, frac={frac:.6f}"
                )
                selected_road_name = str(left["raw"].get("road_name_text") or fuzzy_street_name)
                selected_road_class = left["raw"].get("road_class")
                selected_road_type = left["raw"].get("road_type")
                selected_place_id = left["raw"].get("place_id")
            else:
                endpoints: list[dict[str, Any]] = []
                for row in valid_rows:
                    endpoints.append(
                        {
                            "row": row,
                            "endpoint_num": row["low_num"],
                            "lat": row["low_lat"],
                            "lon": row["low_lon"],
                            "label": "low",
                        }
                    )
                    endpoints.append(
                        {
                            "row": row,
                            "endpoint_num": row["high_num"],
                            "lat": row["high_lat"],
                            "lon": row["high_lon"],
                            "label": "high",
                        }
                    )
                parity_endpoints = [ep for ep in endpoints if ep["row"]["parity_ok"]]
                if parity_endpoints:
                    endpoints = parity_endpoints
                nearest = min(endpoints, key=lambda ep: abs(house_num - ep["endpoint_num"]))
                selected_lat = nearest["lat"]
                selected_lon = nearest["lon"]
                selected_indices = [nearest["row"]["index"]]
                selected_mode = "snapped"
                selected_logic = (
                    "nearest_endpoint_snap: "
                    f"house_num={house_num}, endpoint={nearest['endpoint_num']}, "
                    f"side={nearest['label']}, delta={abs(house_num - nearest['endpoint_num'])}"
                )
                selected_road_name = str(nearest["row"]["raw"].get("road_name_text") or fuzzy_street_name)
                selected_road_class = nearest["row"]["raw"].get("road_class")
                selected_road_type = nearest["row"]["raw"].get("road_type")
                selected_place_id = nearest["row"]["raw"].get("place_id")

        if selected_lat is None or selected_lon is None:
            self.error = "Unable to compute TIGER extrapolated point"
            search_detail["error"] = self.error
            search_detail["result_check"] = "rejected"
            search_detail["result_check_reason"] = "tiger_compute_failed"
            search_detail["result_check_logic"] = selected_logic or "selection yielded no coordinates"
            tiger_meta["error"] = self.error
            return _finalize(False, self.error)

        for idx in selected_indices:
            if idx < len(search_detail["results"]):
                search_detail["results"][idx]["accepted"] = True
                search_detail["results"][idx]["rejection_reason"] = None
                search_detail["results"][idx]["rejection_logic"] = None

        tiger_display_name = f"{house_num}, {selected_road_name}, {zip_code}, TIGER extrapolate/snap"
        lat_text = f"{selected_lat:.7f}"
        lon_text = f"{selected_lon:.7f}"
        self.latitude = lat_text
        self.longitude = lon_text
        self.nominatim_address = tiger_display_name
        self.method = search_name
        self.query = query_text
        self.error = ""
        self.response = tiger_rows
        self.result_metadata = {
            "search_name": search_name,
            "search_query": query_text,
            "search_number_results": len(tiger_rows),
            "accepted_result_index": selected_indices[0] if selected_indices else None,
            "osm_type": None,
            "osm_id": selected_place_id,
            "place_id": selected_place_id,
            "lat": lat_text,
            "lon": lon_text,
            "place_rank": None,
            "class": selected_road_class,
            "type": selected_road_type,
            "addresstype": "tiger_extrapolate_snap",
            "importance": None,
            "bbox_max_dim_m": None,
            "display_name": tiger_display_name,
            "addr_house_number": str(house_num),
            "addr_road": selected_road_name,
            "addr_postcode": zip_code,
            "addr_city": expected_town or None,
            "addr_state": self.address_tags_expanded.get("StateName"),
            "checker_place_rank": None,
            "checker_expected_zip5": self._normalize_zip5(zip_code),
            "checker_result_zip5": self._normalize_zip5(zip_code),
            "checker_zip_match": True,
            "checker_expected_town": expected_town or None,
            "checker_expected_town_normalized": expected_town.casefold() if expected_town else None,
            "checker_town_match": None,
            "checker_town_match_keys": None,
            "checker_reasons": None,
            "tiger_outcome": selected_mode,
            "tiger_logic": selected_logic,
            "tiger_rows_returned": len(tiger_rows),
        }

        search_detail["result_status"] = "returned"
        search_detail["error"] = None
        search_detail["result_check"] = "accepted"
        search_detail["result_check_reason"] = selected_mode
        search_detail["result_check_logic"] = selected_logic
        search_detail["accepted_result_index"] = selected_indices[0] if selected_indices else None
        tiger_meta["outcome"] = selected_mode
        tiger_meta["mode"] = selected_mode
        tiger_meta["error"] = None
        self._log(
            "TIGER extrapolate/snap success: "
            f"mode={selected_mode}, lat={lat_text}, lon={lon_text}, logic={selected_logic}"
        )
        return _finalize(True, "")

    def search(
        self,
        raw_address: str,
        return_metadata: bool = False,
    ) -> "NominatimSearch | tuple[NominatimSearch, Dict[str, Any], Dict[str, Any]]":
        self.reset()
        started_at = time.perf_counter()
        self.raw_address = raw_address.strip()
        self.tag_metadata = {
            "raw_address": self.raw_address,
            "fix_zip_repair": False,
            "fix_state_abbreviation": False,
            "fix_state_abbreviation_before_tags": False,
            "fix_state_abbreviation_after_tags": False,
            "fix_expand_address_abbreviations_count": 0,
            "fix_town_directional": False,
            "fix_address_number_non_numeric": False,
            "reverse_for_state_searched": False,
            "reverse_for_state_included": False,
            "reverse_for_state_number_results": 0,
            "reverse_for_state_all_results_match": None,
            "revers_for_state_display_name": None,
            "address_tags": {},
            "address_tags_expanded": {},
            "missing_street_number": True,
            "missing_street_name": True,
            "missing_city": True,
            "missing_state": True,
            "missing_zip": True,
        }
        self.search_metadata = {
            "raw_address": self.raw_address,
            "bad_address_lookup_used": False,
            "address_cache_used": False,
            "search_attempts": [],
            "search_details": [],
            "search_method_accepted": "none",
            "street_match_in_zip_attempted": False,
            "street_match_in_zip_number_candidates": 0,
            "street_match_in_zip_top_score": None,
            "street_match_in_zip_top_accepted": False,
            "street_match_in_zip_elapsed_ms": 0,
            "tiger_extrapolate_snap": {
                "attempted": False,
                "outcome": "not_attempted",
                "mode": "not_attempted",
                "rows_returned": 0,
                "elapsed_ms": 0,
                "error": None,
            },
            "search_successful": False,
            "final_error": None,
            "elapsed_ms": 0,
            "nominatim_results_returned_total": 0,
            "nominatim_results_returned_by_search": {},
            "nominatim_results_returned_by_method": {},
        }
        self._refresh_process_metadata()

        def _finish() -> "NominatimSearch | tuple[NominatimSearch, Dict[str, Any], Dict[str, Any]]":
            self.search_metadata["search_successful"] = bool(self.latitude and self.longitude)
            self.search_metadata["search_method_accepted"] = (
                self.method if self.search_metadata["search_successful"] and self.method else "none"
            )
            self.search_metadata["final_error"] = self.error or None
            self.search_metadata["elapsed_ms"] = int((time.perf_counter() - started_at) * 1000)
            self._refresh_process_metadata()
            if (
                self.save_address_cache
                and self.raw_address
                and (
                    not self.search_metadata.get("address_cache_used")
                    or self._cache_entry_missing_metadata
                )
            ):
                self._save_address_cache_entry()
            if return_metadata:
                return self, self.tag_metadata, self.search_metadata
            return self

        if not self.raw_address:
            self.error = "Empty address"
            self._log("Empty address received after .strip().")
            return _finish()

        # Bad Address Lookup
        bad_address_update = self._lookup_bad_address(self.raw_address)
        if bad_address_update:
            old_address = self.raw_address
            self.raw_address = bad_address_update
            self.tag_metadata["raw_address"] = self.raw_address
            self.search_metadata["raw_address"] = self.raw_address
            self.search_metadata["bad_address_lookup_used"] = True
            self._log(f"Bad Address: {old_address!r};  Replaced With: {self.raw_address!r}")

        # Address Cache Lookup
        cached_result = self._lookup_address_cache(self.raw_address) if self.use_address_cache else None
        if cached_result:
            self.search_metadata["address_cache_used"] = True
            self._apply_cached_result(cached_result)
            return _finish()

        # Create Tags with usaddress 
        self._parse_address(self.raw_address) # Dict in self.parsed_components in libpostal format
        self.tag_metadata["address_tags"] = dict(self.address_tags_raw)
        self.tag_metadata["address_tags_expanded"] = dict(self.address_tags_expanded)
        self.tag_metadata["missing_street_number"] = not bool(self.address_tags_expanded.get("AddressNumber"))
        self.tag_metadata["missing_street_name"] = not bool(self.address_tags_expanded.get("StreetName"))
        self.tag_metadata["missing_city"] = not bool(self.address_tags_expanded.get("PlaceName"))
        self.tag_metadata["missing_state"] = not bool(self.address_tags_expanded.get("StateName"))
        self.tag_metadata["missing_zip"] = not bool(self.address_tags_expanded.get("ZipCode"))

        # Search Flow:
        # 1) number, street, zip ;
        # 2) number, street, city, state
        # 3) number, fuzzy street, zip
        ####################################################################

        primary_error = ""
        expected_town = self.address_tags_expanded.get("PlaceName", "")
        expected_zip = self.address_tags_expanded.get("ZipCode", "")

        # Search 1: number, street, zip
        search_name = "etags_nsz"
        search_spec = ["AddressNumber", "StreetName", "ZipCode"]
        if all(self.address_tags_expanded.get(tag) for tag in ["StreetName", "ZipCode"]):
            query_parts = self._build_query(search_spec)

            ok, primary_error, search_detail = self._request(
                ", ".join(query_parts),
                search_name,
                expected_zip=expected_zip,
                expected_town=expected_town,
            )
            self._append_search_detail(search_detail)
            if ok:
                return _finish()
        else:
            missing = [
                tag for tag in ["StreetName", "ZipCode"]
                if not self.address_tags_expanded.get(tag)
            ]
            reason = f"missing_required_tags:{','.join(missing)}"
            self._log(f"{search_name}, skipped: {reason}")
            self._append_search_detail(
                self._build_skipped_search_detail(
                    search_name=search_name,
                    reason=reason,
                    expected_zip=expected_zip,
                    expected_town=expected_town,
                )
            )

        # Search 2: number, street, city, state
        search_name = "etags_nscs"
        search_spec = ["AddressNumber", "StreetName", "PlaceName", "StateName"]
        if all(self.address_tags_expanded.get(tag) for tag in ["StreetName", "PlaceName", "StateName"]):
            query_parts = self._build_query(search_spec)

            ok, primary_error, search_detail = self._request(
                ", ".join(query_parts),
                search_name,
                expected_zip=expected_zip,
                expected_town=expected_town,
            )
            self._append_search_detail(search_detail)
            if ok:
                return _finish()
        else:
            missing = [
                tag for tag in ["StreetName", "PlaceName", "StateName"]
                if not self.address_tags_expanded.get(tag)
            ]
            reason = f"missing_required_tags:{','.join(missing)}"
            self._log(f"{search_name}, skipped: {reason}")
            self._append_search_detail(
                self._build_skipped_search_detail(
                    search_name=search_name,
                    reason=reason,
                    expected_zip=expected_zip,
                    expected_town=expected_town,
                )
            )

        # Search 3: number, fuzzy street, zip
        search_name = "zip_street_match_nsz"
        street_value = self._build_street_value()
        number_value = self.address_tags_expanded.get("AddressNumber", "")
        zip_value = self.address_tags_expanded.get("ZipCode", "")
        if not street_value or not zip_value:
            reason = "missing_required_tags:StreetName_or_ZipCode"
            self._log(f"{search_name}, skipped: {reason}")
            self._append_search_detail(
                self._build_skipped_search_detail(
                    search_name=search_name,
                    reason=reason,
                    expected_zip=expected_zip,
                    expected_town=expected_town,
                )
            )
            self.error = primary_error or f"{search_name}, missing street or zip"
            return _finish()

        self.search_metadata["street_match_in_zip_attempted"] = True
        street_match_started_at = time.perf_counter()
        try:
            candidates = self._postcode_candidates(zip_value)
            self.search_metadata["street_match_in_zip_number_candidates"] = len(candidates)
            if self._postcode_lookup_error:
                self._log(f"{search_name}, Postcode lookup error: {self._postcode_lookup_error}")
                self._append_search_detail(
                    self._build_skipped_search_detail(
                        search_name=search_name,
                        reason=f"postcode_lookup_error:{self._postcode_lookup_error}",
                        expected_zip=zip_value,
                        expected_town=expected_town,
                    )
                )
                self.error = f"{primary_error or 'No result'}; {self._postcode_lookup_error}"
                return _finish()
            self._log(f"{search_name}, candidates count: {len(candidates)}")

            match = self._fuzzy_match_road(street_value, candidates)
            self.search_metadata["street_match_in_zip_top_score"] = self._last_fuzzy_top_score
            self.search_metadata["street_match_in_zip_top_accepted"] = bool(match)
            if not match:
                self.error = primary_error or "No fuzzy match for road"
                self._log("No fuzzy match met threshold.")
                self._append_search_detail(
                    self._build_skipped_search_detail(
                        search_name=search_name,
                        reason="no_fuzzy_match",
                        expected_zip=zip_value,
                        expected_town=expected_town,
                    )
                )
                return _finish()
            street_match, score = match

            self._log(f"Using matched road {street_match!r} (score={score}).")
            query_parts = [number_value, street_match, zip_value]
            fuzzy_query = ", ".join([p for p in query_parts if p])
            self.method_outputs = f"match:{street_match!r}, score:{score}, n_candidates:{len(candidates)}"
            self._log(f"{search_name}, query, {fuzzy_query!r}")
            ok, primary_error, search_detail = self._request(
                fuzzy_query,
                search_name,
                expected_zip=zip_value,
                expected_town=expected_town,
            )
            self._append_search_detail(search_detail)
            if ok:
                return _finish()

            self.error = primary_error or self.error
            tiger_ok, tiger_error, tiger_detail = self._search_tiger_extrapolate_snap(
                zip_code=zip_value,
                fuzzy_street_name=street_match,
                address_number=number_value,
                expected_town=expected_town,
            )
            self._append_search_detail(tiger_detail)
            if not tiger_ok:
                self.error = tiger_error or self.error
            return _finish()
        finally:
            self.search_metadata["street_match_in_zip_elapsed_ms"] = int(
                (time.perf_counter() - street_match_started_at) * 1000
            )

        # Unreachable flow-guard return for static analyzers.
        return _finish()
