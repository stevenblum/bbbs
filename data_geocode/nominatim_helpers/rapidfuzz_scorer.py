from __future__ import annotations

import re
from typing import Union

from rapidfuzz import fuzz

NON_ALNUM = re.compile(r"[^a-z0-9]+")

def canon_tokens(s: str) -> str:
    # your existing: lowercase, expand suffix, etc.
    s = s.lower()
    s = NON_ALNUM.sub(" ", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def canon_joined(s: str) -> str:
    # removes separators entirely so Oaklawn == Oak Lawn
    return re.sub(r"[^a-z0-9]", "", s.lower())


def smart_score(user: str, cand: str, **kwargs: object) -> Union[int, float]:
    u_tok = canon_tokens(user)
    c_tok = canon_tokens(cand)

    # Stage 1: token intersection
    s_tok = fuzz.token_set_ratio(u_tok, c_tok)

    # Stage 2: n-gram overlapping
    s_ngram = fuzz.partial_ratio(u_tok, c_tok)

    # Stage 3: joined version
    uj = canon_joined(user)
    cj = canon_joined(cand)
    s_join = fuzz.ratio(uj, cj)

    return max(s_tok, s_ngram, 0.9 * s_join)
