"""Tests the JUP_API_KEY secret against Jupiter. Prints status codes, Jupiter's reply text and the key's shape only."""
import json, os, string, urllib.error, urllib.request

raw = os.environ.get("JUP_API_KEY", "")
key = raw.strip().strip('"').strip("'").strip()
print("raw length:", len(raw), "| stripped length:", len(raw.strip()), "| after removing quotes:", len(key))
print("has newline:", "\n" in raw, "| has inner space:", " " in raw.strip(), "| has quotes:", any(q in raw for q in "\"'"))
print("char classes:", {"lower": any(c in string.ascii_lowercase for c in key), "upper": any(c in string.ascii_uppercase for c in key),
                        "digit": any(c.isdigit() for c in key), "dash": "-" in key, "underscore": "_" in key,
                        "other": any(not (c.isalnum() or c in "-_") for c in key)})
print("looks like UUID (8-4-4-4-12):", [len(p) for p in key.split("-")] == [8, 4, 4, 4, 12])

SOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
tests = [
    ("price v3", "/price/v3?ids=" + SOL),
    ("swap v1 quote", f"/swap/v1/quote?inputMint={SOL}&outputMint={USDC}&amount=100000000&slippageBps=50&swapMode=ExactIn"),
    ("tokens v2 search", "/tokens/v2/search?query=" + SOL),
]


def call(base, path, k):
    h = {"User-Agent": "rage-radium-bot/1.0 (python-urllib)", "Accept": "application/json"}
    if k is not None:
        h["x-api-key"] = k
    try:
        with urllib.request.urlopen(urllib.request.Request(base + path, headers=h), timeout=20) as r:
            body = r.read().decode("utf-8", "ignore")
            rl = {x: r.headers.get(x) for x in ("x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset") if r.headers.get(x)}
            return r.status, body[:140], rl
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore")
        return e.code, body[:300], {}
    except Exception as e:
        return None, type(e).__name__ + ": " + str(e)[:200], {}


for name, path in tests:
    for label, base, k in (("key as stored (stripped)", "https://api.jup.ag", raw.strip()),
                           ("key without quotes", "https://api.jup.ag", key),
                           ("no key, api.jup.ag", "https://api.jup.ag", None),
                           ("no key, lite-api", "https://lite-api.jup.ag", None)):
        if label == "key without quotes" and key == raw.strip():
            continue
        st, body, rl = call(base, path, k)
        if k:
            body = body.replace(k, "***")
        print(f"[{name}] {label}: HTTP {st} {rl} {body!r}")
