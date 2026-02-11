import json
import urllib.parse
import urllib.request

# ---- Edit this ----
QUERY = "20 MAXSON ST, 02804  "
# -------------------

BASE_URL = "http://localhost:8080/search"

params = {
    "q": QUERY,
    "format": "json",
    "addressdetails": 0,
    "limit": 5,
}

url = f"{BASE_URL}?{urllib.parse.urlencode(params)}"
print(f"Request URL: {url}")
req = urllib.request.Request(
    url,
    headers={
        "User-Agent": "nominatim_test/1.0 (local)",
    },
)

try:
    with urllib.request.urlopen(req, timeout=10) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        print(f"Status: {resp.status}")
        try:
            data = json.loads(body)
            print(json.dumps(data, indent=2))
        except json.JSONDecodeError:
            print(body)
except Exception as exc:
    raise SystemExit(f"Request failed: {exc}")
