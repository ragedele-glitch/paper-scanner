"""Rage Radium Bot: a standalone real-money copy-trader with its own signal engine (its own GitHub job, radium.yml).

Since Nov 8 it no longer copies the paper main account. Its own engine (scanner.py run with RADIUM_ENGINE=1, its
own database) follows the same wallets as the paper main account, reads their buys and sells itself, runs the main
account's filters ($25k pool minimum, 32.5% top-10 holder block and the other safety checks) and exits on its own
$20 reference position. When a signal passes, the engine opens a position at once and this process buys; when the
engine sells half at 2x or closes (stop, breakeven, +$5 floor, trail, copied wallet sold), this process sells. The
paper accounts run unchanged in their own job, for comparison. On top of that it has its own guard, so open trades
are never left unwatched while the engine restarts:
  - live prices every ~4 s (DexScreener, Jupiter as backup)
  - its own $5 max-loss line, worked out from its own buy ("$5 is the most I lose")
  - while the engine is down (between hourly segments, between runs): the main account's exits on live prices
    (-25% stop, +15% breakeven, +$5 floor, trail from +40%, half at 2x, breakeven floor) and its own check of the
    copied wallet's balance, so the copied-wallet sell still works

MODES (repository variable RADIUM_MODE):
  off      nothing runs
  dry-run  (default) does everything for real except signing: real Jupiter quotes, the real swap transaction is
           built for the bot's wallet and checked, then thrown away. Nothing is signed or sent. It records what it
           would have done and at what quoted price, next to the engine's $20 reference position.
  live     signs and sends. Needs RADIUM_MODE=live AND the RADIUM_WALLET_KEY secret AND the RADIUM_WALLET_ADDRESS
           secret, and the key must belong to that address. Not set: the user has to fund the wallet, confirm the
           limits and say go first.

ONE TRADER AT A TIME: a lock on the 'radium-lock' branch (compare-and-swap pushes) says which process may trade.
The radium.yml run owns it while the run is alive; near the end of each run a bridge job starts, the old run hands
the lock (and the bot's encrypted state) to it, and the next radium.yml run takes it back. Nobody takes the lock from a
run that GitHub still shows as running, so two traders can't both act.

THE KEY: read once from the environment, kept in memory, never written to disk, a log, the state or the dashboard.
"""
import base64, gzip, json, os, signal, sqlite3, struct, subprocess, sys, tempfile, threading, time, traceback
import urllib.request, urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import config as C

RSTATE = os.environ.get("RADIUM_STATE_DIR", os.path.join(HERE, "radium_state"))
RDB = os.path.join(RSTATE, "radium.db")
RLOG = os.path.join(RSTATE, "radium.log")
STATUS = os.path.join(RSTATE, "status.json")
PIDFILE = os.path.join(RSTATE, "daemon.pid")
RELEASE = os.path.join(RSTATE, "release")
SCANNER_DB = os.environ.get("SCANNER_DB", os.path.join(HERE, "state", "scanner.db"))
CONTROL_FILE = "radium_control.json"

SOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
LAMPORTS = 1_000_000_000
TOKEN_PROGRAMS = ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PxnF2KT1bbbFTi"]
LAGOS = 3600

# hard ceilings: a setting above these is clamped down, whatever the repository variable says
HARD = {"max_stake": 50.0, "max_loss": 15.0, "max_open": 5, "daily_loss": 50.0, "total_loss": 150.0,
        "buy_slip_bps": 2000, "sell_slip_bps": 3000, "priority_lamports": 3_000_000}
SELL_LADDER = [1.0, 1.5, 2.0, 3.0]        # sell retries widen slippage: x the setting, up to the hard cap
SELL_RETRY_S = 6
CONFIRM_TIMEOUT_S = 60
BASE_FEE_LAMPORTS = 5000
LOCK_BRANCH = os.environ.get("RADIUM_LOCK_BRANCH", "radium-lock")
STANDBY_MAX_S = 45 * 60          # a bridge that never gets the lock gives up after this
BRIDGE_MAX_S = 340 * 60          # a bridge that owns the lock dispatches a fresh bridge after this
SCANNER_FRESH_S = 120            # scanner heartbeat newer than this = mirror mode, else guard mode
MIRROR_GRACE_S = 15              # in mirror mode the guard only acts if the paper account hasn't within this
PRICE_EVERY_S = 4
LEADER_EVERY_S = 10
BEAT_EVERY_S = 60


def envf(name, default):
    v = os.environ.get(name, "").strip()
    try:
        return float(v) if v else float(default)
    except ValueError:
        return float(default)


def settings():
    s = {
        "mode": (os.environ.get("RADIUM_MODE", "").strip().lower() or "dry-run"),
        "max_loss": min(envf("RADIUM_MAX_LOSS_USD", 5), HARD["max_loss"]),
        "max_stake": min(envf("RADIUM_MAX_STAKE_USD", 20), HARD["max_stake"]),
        "min_stake": envf("RADIUM_MIN_STAKE_USD", 8),
        "stop_pct": C.MAX_LOSS_PCT * 100,
        "max_open": int(min(envf("RADIUM_MAX_OPEN", 3), HARD["max_open"])),
        "daily_loss": min(envf("RADIUM_DAILY_LOSS_USD", 15), HARD["daily_loss"]),
        "total_loss": min(envf("RADIUM_TOTAL_LOSS_USD", 30), HARD["total_loss"]),
        "buy_slip_bps": int(min(envf("RADIUM_BUY_SLIPPAGE_BPS", 1000), HARD["buy_slip_bps"])),
        "sell_slip_bps": int(min(envf("RADIUM_SELL_SLIPPAGE_BPS", 1500), HARD["sell_slip_bps"])),
        "max_impact": envf("RADIUM_MAX_IMPACT_PCT", 5),
        "max_lag": envf("RADIUM_MAX_LAG_S", 30),
        "priority_lamports": int(min(envf("RADIUM_PRIORITY_MAX_LAMPORTS", 1_000_000), HARD["priority_lamports"])),
        "reserve_sol": envf("RADIUM_FEE_RESERVE_SOL", 0.03),
        "jup_key": os.environ.get("JUP_API_KEY", "").strip(),
    }
    if s["mode"] not in ("off", "dry-run", "live"):
        s["mode"] = "dry-run"
    hk = os.environ.get("HELIUS_API_KEY", "").strip()
    s["rpc"] = os.environ.get("RADIUM_RPC_URL", "").strip() or (
        f"https://mainnet.helius-rpc.com/?api-key={hk}" if hk else "https://api.mainnet-beta.solana.com")
    return s


S = settings()
WALLET = {"address": None, "kp": None, "placeholder": None}
REDACT = []          # strings that must never reach the log or the dashboard


def load_wallet():
    """Address from the RADIUM_WALLET_ADDRESS secret. The key is only read in live mode; in dry-run it is dropped."""
    addr = os.environ.pop("RADIUM_WALLET_ADDRESS", "").strip() or None
    raw = os.environ.pop("RADIUM_WALLET_KEY", "").strip()
    WALLET["address"] = addr
    if raw:
        REDACT.append(raw)
    if addr:
        REDACT.append(addr)
    WALLET["key_present"] = bool(raw)
    WALLET["key_ok"] = None
    if raw:
        try:
            kp = keypair_from(raw)
            WALLET["key_ok"] = (addr is not None and str(kp.pubkey()) == addr)
            if S["mode"] == "live" and WALLET["key_ok"]:
                WALLET["kp"] = kp
        except Exception:
            WALLET["key_ok"] = False
    raw = None


def keypair_from(raw):
    """Phantom's 'Show private key' gives base58 (64 bytes); a JSON byte list (Solana CLI file) also works."""
    from solders.keypair import Keypair
    raw = raw.strip().strip('"')
    if raw.startswith("["):
        return Keypair.from_bytes(bytes(json.loads(raw)))
    return Keypair.from_base58_string(raw)


def signer_address():
    """The wallet the swap transactions are built for: the bot's wallet, or (no wallet set yet) a throwaway one."""
    if WALLET["address"]:
        return WALLET["address"]
    if not WALLET["placeholder"]:
        from solders.keypair import Keypair
        WALLET["placeholder"] = str(Keypair().pubkey())   # never funded, never used to sign
    return WALLET["placeholder"]


def scrub(x):
    t = str(x)
    for r in REDACT:
        if r and r in t:
            t = t.replace(r, "[hidden]")
    return t


def log(msg):
    os.makedirs(RSTATE, exist_ok=True)
    line = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) + " " + scrub(msg)
    with open(RLOG, "a") as f:
        f.write(line + "\n")
    if os.path.exists(RLOG) and os.path.getsize(RLOG) > 3_000_000:
        with open(RLOG, "rb") as f:
            f.seek(-2_000_000, 2); tail = f.read()
        with open(RLOG, "wb") as f:
            f.write(tail[tail.find(b"\n") + 1:])


# ----------------------------------------------------------------------------- database
SCHEMA = """
CREATE TABLE IF NOT EXISTS trades(id INTEGER PRIMARY KEY, paper_id INT UNIQUE, mint TEXT, symbol TEXT, pool TEXT,
  leader TEXT, mode TEXT, status TEXT, created_ts REAL, paper_entry_ts REAL, paper_entry_px REAL, paper_size REAL,
  lag_s REAL, stake_usd REAL, sol_in REAL, sol_px REAL, tokens0 TEXT, tokens TEXT, decimals INT, mid_px REAL,
  entry_px REAL, rt_pct REAL, impact_pct REAL, fee_in_usd REAL, buy_sig TEXT, buy_note TEXT,
  paper_stop REAL, paper_be REAL, paper_floor REAL, paper_peak REAL, peak REAL, gstop REAL,
  last_px REAL, last_px_ts REAL, below_since REAL, leader_base TEXT, leader_chk REAL,
  half_ts REAL, half_usd REAL, half_sig TEXT, exit_ts REAL, exit_reason TEXT, exit_usd REAL, fee_out_usd REAL,
  sell_sig TEXT, sell_tries INT DEFAULT 0, next_try REAL, last_err TEXT, pnl_usd REAL,
  paper_status TEXT, paper_exit_reason TEXT, paper_pnl REAL, notes TEXT);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, ts REAL, kind TEXT, msg TEXT);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
"""


def db():
    os.makedirs(RSTATE, exist_ok=True)
    con = sqlite3.connect(RDB, timeout=30)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    return con


def meta_get(con, k, d=None):
    r = con.execute("SELECT value FROM meta WHERE key=?", (k,)).fetchone()
    return json.loads(r[0]) if r else d


def meta_set(con, k, v):
    con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (k, json.dumps(v)))


def event(con, kind, msg):
    msg = scrub(msg)
    con.execute("INSERT INTO events(ts,kind,msg) VALUES(?,?,?)", (time.time(), kind, msg))
    con.execute("DELETE FROM events WHERE id < (SELECT MAX(id) FROM events) - 400")
    con.commit()
    log(f"[{kind}] {msg}")
    DIRTY[0] = True


DIRTY = [False]


def db_bytes(con):
    con.commit()
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "b.db")
        b = sqlite3.connect(p)
        con.backup(b)
        b.close()
        with open(p, "rb") as f:
            return f.read()


# ----------------------------------------------------------------------------- encryption (same key as the dashboard)
def _aes():
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    k = os.environ["DASH_KEY"].strip()
    return AESGCM(base64.urlsafe_b64decode(k + "=" * (-len(k) % 4)))


def seal(b):
    n = os.urandom(12)
    return base64.b64encode(n + _aes().encrypt(n, gzip.compress(b, 6), None))


def unseal(b):
    raw = base64.b64decode(b)
    return gzip.decompress(_aes().decrypt(raw[:12], raw[12:], None))


# ----------------------------------------------------------------------------- HTTP, Jupiter, RPC, prices
UA = "rage-radium-bot/1.0 (python-urllib)"
_J = {"last": 0.0, "sol": (0.0, 0.0), "dec": {}}


def http(url, body=None, headers=None, timeout=25, tries=3):
    h = {"User-Agent": UA, "Accept": "application/json"}
    if body is not None:
        h["Content-Type"] = "application/json"
    h.update(headers or {})
    data = json.dumps(body).encode() if body is not None else None
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=h), timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and i < tries - 1:
                time.sleep(1.5 * (i + 1)); continue
            detail = ""
            try:
                detail = e.read().decode("utf-8", "ignore")[:160]
            except Exception:
                pass
            raise RuntimeError(f"HTTP {e.code} from {url.split('?')[0]} {detail}") from None
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if i < tries - 1:
                time.sleep(1.0 * (i + 1)); continue
            raise RuntimeError(f"network error {type(e).__name__} from {url.split('?')[0]}") from None


def jup(path, params=None, body=None):
    gap = 1.05 if S["jup_key"] else 1.1
    w = _J["last"] + gap - time.time()
    if w > 0:
        time.sleep(w)
    _J["last"] = time.time()
    base = "https://api.jup.ag" if S["jup_key"] else "https://lite-api.jup.ag"
    url = base + path + ("?" + "&".join(f"{k}={v}" for k, v in params.items()) if params else "")
    return http(url, body, {"x-api-key": S["jup_key"]} if S["jup_key"] else None)


def rpc(method, params):
    j = http(S["rpc"], {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=30)
    if "error" in j:
        raise RuntimeError(f"rpc {method}: {str(j['error'])[:160]}")
    return j["result"]


def quote(inp, out, amount_raw, slip_bps):
    q = jup("/swap/v1/quote", {"inputMint": inp, "outputMint": out, "amount": int(amount_raw),
                                "slippageBps": int(slip_bps), "swapMode": "ExactIn"})
    if not q or not q.get("outAmount") or not q.get("routePlan"):
        raise RuntimeError("Jupiter found no route")
    return q


def route_label(q):
    return " > ".join(dict.fromkeys(r.get("swapInfo", {}).get("label", "?") for r in q.get("routePlan", [])))


def impact(q):
    return abs(float(q.get("priceImpactPct") or 0)) * 100


def sol_usd():
    t, px = _J["sol"]
    if time.time() - t < 60 and px > 0:
        return px
    try:
        j = jup("/price/v3", {"ids": SOL})
        px = float(j[SOL]["usdPrice"])
    except Exception:
        q = quote(SOL, USDC, LAMPORTS, 50)
        px = int(q["outAmount"]) / 1e6
    _J["sol"] = (time.time(), px)
    return px


def decimals(mint):
    if mint not in _J["dec"]:
        _J["dec"][mint] = int(rpc("getTokenSupply", [mint])["value"]["decimals"])
    return _J["dec"][mint]


PX_SEEN = {}       # mint -> {source: [value, ts the value last changed]}: spots a frozen (cached) price source
PX_STALE_S = 15    # same rule as the paper scanner's live checks (Nov 8 fix): a price unchanged this long is stale


def prices(mints, pools=None):
    """{mint: usd price}. Asks Jupiter and DexScreener every time and uses the freshest: the source whose value
    changed most recently, Jupiter on a tie. Nov 8 fix (same as the paper scanner's live checks): DexScreener caches
    prices for ~30 s and lagged the 'invest' trade, so a source whose price has not changed for more than 15 s is
    treated as stale and the other one is used. If both are stale, the most recently changed one is still used,
    so the stops keep working on a quiet coin, and the guard notes it."""
    pools = pools or {}
    mints = list(dict.fromkeys(mints))
    jup, dex = {}, {}
    try:
        j = http("https://lite-api.jup.ag/price/v3?ids=" + ",".join(mints[:50]), timeout=10, tries=1) or {}
        for m in mints[:50]:
            if j.get(m) and j[m].get("usdPrice") and float(j[m]["usdPrice"]) > 0:
                jup[m] = float(j[m]["usdPrice"])
    except Exception:
        pass
    for i in range(0, len(mints), 30):
        part = mints[i:i + 30]
        try:
            pairs = http("https://api.dexscreener.com/tokens/v1/solana/" + ",".join(part), timeout=10, tries=1)
            best = {}
            for p in pairs or []:
                m = (p.get("baseToken") or {}).get("address")
                if m in part and p.get("priceUsd"):
                    liq = ((p.get("liquidity") or {}).get("usd") or 0)
                    if pools.get(m) and p.get("pairAddress") == pools[m]:
                        liq = 1e18
                    if m not in best or liq > best[m][0]:
                        best[m] = (liq, float(p["priceUsd"]))
            dex.update({m: v[1] for m, v in best.items() if v[1] > 0})
        except Exception:
            pass
    t = time.time()
    out = {}
    for m in mints:
        cand = {k: v for k, v in (("jupiter", jup.get(m)), ("dexscreener", dex.get(m))) if v}
        if not cand:
            continue
        seen = PX_SEEN.setdefault(m, {})
        for src, v in cand.items():
            if src not in seen or seen[src][0] != v:
                seen[src] = [v, t]
        ranked = sorted((t - seen[src][1], 0 if src == "jupiter" else 1, src) for src in cand)
        out[m] = cand[ranked[0][2]]
        PX_SEEN[m]["_pick"] = [ranked[0][2], ranked[0][0] > PX_STALE_S]
    for m in list(PX_SEEN):
        if m not in mints and len(PX_SEEN) > 200:
            PX_SEEN.pop(m, None)
    return out


def sol_balance(addr):
    return rpc("getBalance", [addr, {"commitment": "confirmed"}])["value"] / LAMPORTS


def token_balance(owner, mint):
    res = rpc("getTokenAccountsByOwner", [owner, {"mint": mint}, {"encoding": "jsonParsed", "commitment": "confirmed"}])
    return sum(int(a["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]) for a in res["value"])


def token_accounts(owner):
    """[(account address, mint, raw amount, program)] for every token account the wallet owns."""
    out = []
    for prog in TOKEN_PROGRAMS:
        res = rpc("getTokenAccountsByOwner", [owner, {"programId": prog}, {"encoding": "jsonParsed", "commitment": "confirmed"}])
        for a in res["value"]:
            info = a["account"]["data"]["parsed"]["info"]
            out.append((a["pubkey"], info["mint"], int(info["tokenAmount"]["amount"]), prog))
    return out


# ----------------------------------------------------------------------------- building, checking, signing, sending
def build_swap(q):
    """Asks Jupiter for the swap transaction for our wallet and checks it. Returns the unsigned transaction."""
    from solders.transaction import VersionedTransaction
    body = {"quoteResponse": q, "userPublicKey": signer_address(), "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "prioritizationFeeLamports": {"priorityLevelWithMaxLamports": {"maxLamports": S["priority_lamports"],
                                                                           "priorityLevel": "veryHigh"}}}
    r = jup("/swap/v1/swap", body=body)
    tx = VersionedTransaction.from_bytes(base64.b64decode(r["swapTransaction"]))
    msg = tx.message
    if msg.header.num_required_signatures != 1 or str(msg.account_keys[0]) != signer_address():
        raise RuntimeError("Jupiter's transaction doesn't have the bot's wallet as its only signer")
    return tx, int(r.get("prioritizationFeeLamports") or 0)


def simulate(tx):
    """Dry run: asks the chain what would happen, without a signature (the wallet being empty shows up here)."""
    try:
        r = rpc("simulateTransaction", [base64.b64encode(bytes(tx)).decode(),
                                        {"encoding": "base64", "sigVerify": False, "replaceRecentBlockhash": True,
                                         "commitment": "processed"}])
        err = r["value"].get("err")
        if not err:
            return "simulation ok"
        logs = " ".join(r["value"].get("logs") or [])
        if "insufficient" in logs.lower() or "InsufficientFunds" in str(err) or "0x1" in str(err):
            return "simulation: not enough funds in the wallet (expected while it's empty)"
        return f"simulation error: {str(err)[:80]}"
    except Exception as e:
        return f"simulation not run: {str(e)[:60]}"


def sign_send(tx):
    """LIVE ONLY. Signs with the wallet key and sends. Returns the signature once confirmed; raises otherwise."""
    if S["mode"] != "live" or WALLET["kp"] is None or not OWNER.get("ok"):
        raise RuntimeError("refusing to sign: not live, no key, or not the lock owner")
    from solders.transaction import VersionedTransaction
    signed = VersionedTransaction(tx.message, [WALLET["kp"]])
    raw = base64.b64encode(bytes(signed)).decode()
    sig = rpc("sendTransaction", [raw, {"encoding": "base64", "skipPreflight": True, "maxRetries": 0}])
    t0 = time.time(); last_send = t0
    while time.time() - t0 < CONFIRM_TIMEOUT_S:
        time.sleep(1.5)
        st = rpc("getSignatureStatuses", [[sig]])["value"][0]
        if st and st.get("err"):
            raise RuntimeError(f"failed on-chain: {str(st['err'])[:80]}")
        if st and st.get("confirmationStatus") in ("confirmed", "finalized"):
            return sig
        if time.time() - last_send > 4:      # re-broadcast the same signed transaction until it lands or expires
            try:
                rpc("sendTransaction", [raw, {"encoding": "base64", "skipPreflight": True, "maxRetries": 0}])
            except Exception:
                pass
            last_send = time.time()
    raise RuntimeError(f"not confirmed after {CONFIRM_TIMEOUT_S}s")


def fill_of(sig, mint):
    """Actual fill of a confirmed swap: (SOL change incl. fees, token change raw, network fee SOL)."""
    for _ in range(10):
        tx = rpc("getTransaction", [sig, {"encoding": "jsonParsed", "commitment": "confirmed",
                                           "maxSupportedTransactionVersion": 0}])
        if tx:
            m = tx["meta"]
            keys = [k["pubkey"] if isinstance(k, dict) else k for k in tx["transaction"]["message"]["accountKeys"]]
            i = keys.index(WALLET["address"])
            dsol = (m["postBalances"][i] - m["preBalances"][i]) / LAMPORTS
            def tok(bals):
                return sum(int(b["uiTokenAmount"]["amount"]) for b in bals or []
                           if b.get("owner") == WALLET["address"] and b.get("mint") == mint)
            return dsol, tok(m.get("postTokenBalances")) - tok(m.get("preTokenBalances")), m["fee"] / LAMPORTS
        time.sleep(2)
    raise RuntimeError("confirmed transaction could not be read back")


def close_empty_accounts():
    """LIVE ONLY. Closes the bot's empty token accounts so their ~0.002 SOL deposit comes back."""
    if S["mode"] != "live" or WALLET["kp"] is None or not OWNER.get("ok"):
        return 0
    from solders.pubkey import Pubkey
    from solders.instruction import Instruction, AccountMeta
    from solders.message import MessageV0
    from solders.transaction import VersionedTransaction
    from solders.hash import Hash
    owner = WALLET["kp"].pubkey()
    empty = [(a, p) for a, m, amt, p in token_accounts(WALLET["address"]) if amt == 0 and m != SOL]
    if not empty:
        return 0
    ixs = [Instruction(Pubkey.from_string(p), bytes([9]),
                       [AccountMeta(Pubkey.from_string(a), False, True), AccountMeta(owner, False, True),
                        AccountMeta(owner, True, False)]) for a, p in empty[:8]]
    bh = Hash.from_string(rpc("getLatestBlockhash", [{"commitment": "confirmed"}])["value"]["blockhash"])
    tx = VersionedTransaction(MessageV0.try_compile(owner, ixs, [], bh), [WALLET["kp"]])
    sig = rpc("sendTransaction", [base64.b64encode(bytes(tx)).decode(), {"encoding": "base64"}])
    return len(ixs) if sig else 0


# ----------------------------------------------------------------------------- the lock (one trader at a time)
OWNER = {"ok": False, "sha": None, "lock": None, "checked": 0.0}
RUN_ID = os.environ.get("GITHUB_RUN_ID") or os.environ.get("RADIUM_RUN_ID") or f"local-{os.getpid()}"
REPO = os.environ.get("GITHUB_REPOSITORY", "")
TOKEN = os.environ.get("GITHUB_TOKEN", "")


class Lock:
    """lock.json + state.enc on their own branch. Every change is a push that only lands if the branch is still at
    the commit we last saw (git push --force-with-lease=<branch>:<sha>), so two processes can't both change it."""

    def __init__(self, branch=LOCK_BRANCH):
        self.branch = branch
        self.ref = f"refs/heads/{branch}"
        self.dir = os.path.join(RSTATE, "lockrepo")
        if not os.path.isdir(os.path.join(self.dir, ".git")):
            os.makedirs(self.dir, exist_ok=True)
            self.git("init", "-q")
            self.git("config", "user.name", "rage-radium-bot")
            self.git("config", "user.email", "rage-radium-bot@users.noreply.github.com")
            remote = os.environ.get("RADIUM_LOCK_REMOTE") or f"https://x-access-token:{TOKEN}@github.com/{REPO}.git"
            self.git("remote", "add", "origin", remote)

    def git(self, *a, data=None, text=True):
        return subprocess.run(["git", *a], cwd=self.dir, input=data, capture_output=True, text=text, timeout=60)

    def head(self):
        """The branch's commit on GitHub right now (None if the branch doesn't exist). Raises on network failure."""
        r = self.git("ls-remote", "origin", self.ref)
        if r.returncode != 0:
            raise RuntimeError("lock read failed")
        line = r.stdout.strip()
        return line.split()[0] if line else None

    def read(self):
        sha = self.head()
        if not sha:
            return None, None, None
        r = self.git("fetch", "-q", "origin", f"+{self.ref}:refs/remotes/origin/{self.branch}")
        if r.returncode != 0:
            raise RuntimeError("lock fetch failed")
        sha = self.git("rev-parse", f"refs/remotes/origin/{self.branch}").stdout.strip()
        lk = json.loads(self.git("show", f"{sha}:lock.json").stdout)
        blob = self.git("show", f"{sha}:state.enc", text=False).stdout or None
        return sha, lk, blob

    def write(self, expected, lk, blob):
        """Returns the new commit, or None if someone else changed the branch first."""
        def obj(b):
            return self.git("hash-object", "-w", "--stdin", data=b, text=False).stdout.decode().strip()
        a = obj(json.dumps(lk, indent=1).encode())
        b = obj(blob or b"")
        tree = self.git("mktree", data=f"100644 blob {a}\tlock.json\n100644 blob {b}\tstate.enc\n").stdout.strip()
        c = self.git("commit-tree", tree, "-m", f"radium lock: {lk.get('owner')} ({lk.get('role')}) seq {lk.get('seq')}").stdout.strip()
        r = self.git("push", "-q", f"--force-with-lease={self.ref}:{expected or ''}", "origin", f"{c}:{self.ref}")
        return c if r.returncode == 0 else None


def run_alive(run_id):
    """Is that GitHub Actions run still going? Unsure (API error) counts as alive, so the lock is never stolen."""
    if not run_id:
        return False
    if str(run_id) == RUN_ID:
        return True
    test = os.environ.get("RADIUM_TEST_RUNS")
    if test:   # local tests: a json file {run_id: true/false}
        try:
            with open(test) as f:
                return bool(json.load(f).get(str(run_id), False))
        except Exception:
            return True
    try:
        j = http(f"https://api.github.com/repos/{REPO}/actions/runs/{run_id}",
                 headers={"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json"}, timeout=15, tries=2)
        return j.get("status") != "completed"
    except RuntimeError as e:
        if "HTTP 404" in str(e):
            return False
        return True


def dispatch(workflow):
    if os.environ.get("RADIUM_TEST_RUNS"):
        log(f"(test) would dispatch {workflow}"); return True
    req = urllib.request.Request(
        f"https://api.github.com/repos/{REPO}/actions/workflows/{workflow}/dispatches",
        data=json.dumps({"ref": "main"}).encode(), method="POST",
        headers={"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28", "User-Agent": UA})
    try:
        urllib.request.urlopen(req, timeout=20)
        return True
    except Exception as e:
        log(f"dispatch {workflow} failed: {type(e).__name__}")
        return False


# ----------------------------------------------------------------------------- the control file (phone switch)
_CTL = {"t": 0.0, "ok_t": time.time(), "v": {}}


def control():
    """radium_control.json on main: {"pause": bool, "sell_all_and_stop": bool}. Re-read every 15 s. If it can't
    be read for 10 minutes the bot pauses new buys (it keeps managing open trades)."""
    if time.time() - _CTL["t"] >= 15:
        _CTL["t"] = time.time()
        try:
            if REPO and TOKEN and not os.environ.get("RADIUM_TEST_RUNS"):
                j = http(f"https://api.github.com/repos/{REPO}/contents/{CONTROL_FILE}?ref=main",
                         headers={"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github.raw+json"},
                         timeout=10, tries=1)
            else:
                with open(os.path.join(HERE, CONTROL_FILE)) as f:
                    j = json.load(f)
            _CTL["v"] = j if isinstance(j, dict) else {}
            _CTL["ok_t"] = time.time()
        except Exception:
            pass
    v = dict(_CTL["v"])
    v["stale"] = time.time() - _CTL["ok_t"] > 600
    return v


# ----------------------------------------------------------------------------- exits (same rules as the main account)
FLOOR_SPEC = dict(max_loss=C.MAX_LOSS_PCT, be=C.BE2_TRIGGER, half_at=C.HALF_AT, trail=C.TRAIL_V3,
                  be15=C.BE15_TRIGGER, floor=True)


def stop_for(spec, entry, peak, old, be_px, floor_px=None):
    """Copy of scanner.stop_for (main account rules), used while the scanner is down."""
    gain = peak / entry - 1 + 1e-9
    s = entry * (1 - spec["max_loss"]) if spec.get("max_loss") else 0.0
    for trig, dist in spec["trail"]:
        if gain >= trig:
            s = peak * (1 - dist)
    if spec.get("be") and gain >= spec["be"]:
        s = max(s, be_px or entry * C.BE_LEVEL)
    if spec.get("be15") and gain >= spec["be15"]:
        b = be_px or entry * C.BE_LEVEL
        if peak >= b:
            s = max(s, b)
    if spec.get("floor") and floor_px and peak >= floor_px:
        s = max(s, floor_px)
    return max(old or 0, s)


def stake_for(rt_pct, sol_px):
    """Stake so that the -25% stop loses about RADIUM_MAX_LOSS_USD including this trade's own costs:
    loss at stop = stake x (25% + round-trip cost %) + network fees for both legs. Never above the max stake."""
    fees = 2 * (BASE_FEE_LAMPORTS + S["priority_lamports"]) / LAMPORTS * sol_px
    rt = max(0.0, (rt_pct or 3.0) / 100)
    st = (S["max_loss"] - fees) / (S["stop_pct"] / 100 + rt)
    return round(min(S["max_stake"], st), 2), fees


def col(row, k):
    """A column of a scanner row, or None if that database doesn't have it (older copies lack floor_px)."""
    try:
        return row[k] if k in row.keys() else None
    except Exception:
        return None


def lagos_day_start(t=None):
    t = t or time.time()
    return (t + LAGOS) // 86400 * 86400 - LAGOS


# ----------------------------------------------------------------------------- the bot
class Bot:
    def __init__(self, role):
        self.role = role
        self.con = None
        self.lock = Lock()
        self.started = time.time()
        self.last_px = 0.0
        self.last_beat = 0.0
        self.last_status = 0.0
        self.last_save = 0.0
        self.bal = {"sol": None, "t": 0.0, "err": None}
        self.want_sent = False
        self.next_bridge_sent = False
        self.mode_now = "starting"
        self.release_seen = None

    # ---- lock handling
    def blob(self):
        return seal(db_bytes(self.con)) if self.con else None

    def lock_body(self, owner, role, seq, want=None, note=None):
        return {"v": 1, "owner": owner, "role": role, "seq": seq, "beat": time.time(), "want": want,
                "mode": S["mode"], "note": note}

    def acquire(self):
        """Blocks until this process owns the lock (or gives up, for a bridge). Returns True when owner."""
        t0 = time.time()
        while True:
            if os.path.exists(RELEASE):
                return False
            try:
                sha, lk, blob = self.lock.read()
            except Exception as e:
                log(f"lock read error {e}"); time.sleep(3); continue
            owner = (lk or {}).get("owner")
            free = (lk is None or owner is None or str(owner) == RUN_ID or not run_alive(owner))
            if free:
                same = lk is not None and str(owner) == RUN_ID
                self.open_db(blob, prefer_local=same)
                seq = ((lk or {}).get("seq") or 0) + 1
                new = self.lock.write(sha, self.lock_body(RUN_ID, self.role, seq), self.blob())
                if new:
                    OWNER.update(ok=True, sha=new, checked=time.time(), lock=self.lock_body(RUN_ID, self.role, seq))
                    how = ("nobody held it" if not owner else
                           ("handed over to it" if same and (lk or {}).get("note") else
                            ("restarted in the same run" if same else f"run {owner} had ended")))
                    event(self.con, "info", f"took the trading lock ({self.role}, run {RUN_ID}): {how}")
                    return True
                continue
            cw = lk.get("want") or {}
            if str(cw.get("run")) == RUN_ID and cw.get("role") == self.role:
                pass   # already asked
            elif self.role == "primary" or not cw or cw.get("role") != "primary" or not run_alive(cw.get("run")):
                # a scan run always asks; a bridge never pushes aside a scan run that already asked
                w = {"run": RUN_ID, "role": self.role, "ts": time.time()}
                cur = dict(lk); cur["want"] = w
                self.lock.write(sha, cur, blob)
            if self.role == "bridge" and time.time() - t0 > STANDBY_MAX_S:
                log("bridge: never needed, leaving")
                return False
            time.sleep(2)

    def check_owner(self):
        """Every ~2 s: is the branch still at our last write? If someone else wrote, re-read."""
        if time.time() - OWNER["checked"] < 2:
            return OWNER["ok"]
        try:
            h = self.lock.head()
        except Exception:
            return OWNER["ok"]      # network blip: keep the last answer, the next write will tell
        OWNER["checked"] = time.time()
        if h == OWNER["sha"]:
            return True
        try:
            sha, lk, blob = self.lock.read()
        except Exception:
            return OWNER["ok"]
        if lk and str(lk.get("owner")) == RUN_ID and lk.get("role") == self.role:
            OWNER.update(sha=sha, lock=lk)       # someone added a 'want'
            return True
        OWNER["ok"] = False
        event(self.con, "bad", f"lost the trading lock to {lk.get('owner') if lk else 'nobody'}: stopping")
        return False

    def save(self, note=None, owner=None, role=None, want="keep"):
        """Writes our state (and maybe a new owner) to the lock branch. Returns True if it landed."""
        for _ in range(4):
            lk = OWNER["lock"] or {}
            body = self.lock_body(RUN_ID if owner is None else owner, self.role if role is None else role,
                                  (lk.get("seq") or 0) + (1 if owner is not None else 0),
                                  lk.get("want") if want == "keep" else want, note)
            if owner == "":
                body["owner"] = None
            new = self.lock.write(OWNER["sha"], body, self.blob())
            if new:
                OWNER.update(sha=new, lock=body, checked=time.time())
                self.last_save = time.time(); DIRTY[0] = False
                return True
            try:
                sha, lk2, _ = self.lock.read()
            except Exception:
                time.sleep(1); continue
            if lk2 and str(lk2.get("owner")) == RUN_ID and lk2.get("role") == self.role:
                OWNER.update(sha=sha, lock=lk2); continue     # a 'want' arrived in between: retry on top of it
            OWNER["ok"] = False
            return False
        return False

    def handover(self, why):
        """Give the lock (with the state) to whoever asked for it, or release it if nobody did."""
        self.write_status()
        want = (OWNER["lock"] or {}).get("want")
        if want:
            ok = self.save(note=why, owner=str(want["run"]), role=want["role"], want=None)
            event(self.con, "info", f"handed the trading lock to {want['role']} run {want['run']} ({why})" if ok
                  else "hand-over write failed")
        else:
            ok = self.save(note=why, owner="", role=None, want=None)
            event(self.con, "info", f"released the trading lock ({why}); the next run picks it up" if ok
                  else "release write failed")
        OWNER["ok"] = False
        try:
            self.save_noop()
        except Exception:
            pass

    def save_noop(self):
        pass

    # ---- database
    def open_db(self, blob, prefer_local):
        if self.con:
            self.con.close(); self.con = None
        if blob and not (prefer_local and os.path.exists(RDB)):
            try:
                data = unseal(blob)
                os.makedirs(RSTATE, exist_ok=True)
                for ext in ("", "-wal", "-shm", "-journal"):
                    if os.path.exists(RDB + ext):
                        os.remove(RDB + ext)
                with open(RDB, "wb") as f:
                    f.write(data)
            except Exception as e:
                log(f"state restore failed: {type(e).__name__}")
        self.con = db()
        if meta_get(self.con, "since") is None:
            meta_set(self.con, "since", time.time()); self.con.commit()
        src = os.environ.get("RADIUM_SOURCE", "engine")
        if meta_get(self.con, "source") != src:
            # Nov 8: switched from copying the paper main account to its own engine. Position ids now come from a
            # different database, so old rows are set aside (negative id, never matched again; finished ones leave
            # the totals). Anything still open stays open and is guarded on live prices.
            self.con.execute("UPDATE trades SET paper_id=-ABS(paper_id)")
            self.con.execute("UPDATE trades SET mode='old-'||mode WHERE status IN ('closed','skipped','failed')")
            meta_set(self.con, "since", time.time()); meta_set(self.con, "source", src); self.con.commit()
            event(self.con, "info", "Now runs on its own signal engine (no longer a copy of the paper main account)")

    # ---- the scanner's main account
    def scanner(self):
        if not os.path.exists(SCANNER_DB):
            return None, None
        try:
            sc = sqlite3.connect(f"file:{SCANNER_DB}?mode=ro", uri=True, timeout=5)
            sc.row_factory = sqlite3.Row
            hb = sc.execute("SELECT MAX(ts) FROM heartbeat").fetchone()[0]
            return sc, hb
        except Exception:
            return None, None

    # ---- money checks
    def balance(self):
        if not WALLET["address"]:
            return None
        if time.time() - self.bal["t"] > 60:
            self.bal["t"] = time.time()
            try:
                self.bal["sol"] = sol_balance(WALLET["address"]); self.bal["err"] = None
                sol_usd()
            except Exception as e:
                self.bal["err"] = str(e)[:80]
        return self.bal["sol"]

    def realised(self, since):
        r = self.con.execute("SELECT COALESCE(SUM(pnl_usd),0) FROM trades WHERE status='closed' AND exit_ts>=? AND mode=?",
                             (since, S["mode"])).fetchone()[0]
        return r or 0.0

    def open_trades(self):
        return self.con.execute("SELECT * FROM trades WHERE status IN ('buying','open','closing') ORDER BY id").fetchall()

    def blocked(self, ctl):
        if S["mode"] == "off":
            return "bot is off"
        if ctl.get("sell_all_and_stop"):
            return "stopped from the phone switch (sell everything and stop)"
        if ctl.get("pause"):
            return "paused from the phone switch"
        if ctl.get("stale"):
            return "couldn't read the phone switch for 10 minutes"
        if S["mode"] == "live" and WALLET["address"]:
            bal, px = self.bal["sol"], _J["sol"][1]
            if bal is not None and px:
                need = S["max_stake"] / px + S["reserve_sol"]
                if bal < need:
                    return f"wallet has {bal:.4f} SOL, below one trade plus fees ({need:.4f} SOL)"
        if len(self.open_trades()) >= S["max_open"]:
            return f"already {S['max_open']} open trades (max)"
        day = self.realised(lagos_day_start())
        if day <= -S["daily_loss"]:
            return f"daily loss cap hit (${-day:.2f} lost today, cap ${S['daily_loss']:.0f})"
        tot = self.realised(meta_get(self.con, "loss_count_from", 0))
        if tot <= -S["total_loss"]:
            return f"total loss cap hit (${-tot:.2f}, cap ${S['total_loss']:.0f})"
        return None

    # ---- buying
    def mirror_buys(self, sc):
        since = meta_get(self.con, "since", time.time())
        rows = sc.execute("""SELECT * FROM positions WHERE account=? AND entry_ts>=? ORDER BY entry_ts""",
                          (C.ACCT_REAL, since)).fetchall()
        done = {r[0] for r in self.con.execute("SELECT paper_id FROM trades")}
        for p in rows:
            if p["id"] in done:
                continue
            self.buy(p)

    def buy(self, p):
        t = time.time()
        p = dict(p)
        lag = t - (p["entry_ts"] or t)
        cur = self.con.execute("""INSERT INTO trades(paper_id,mint,symbol,pool,leader,mode,status,created_ts,paper_entry_ts,
                                  paper_entry_px,paper_size,lag_s,paper_stop,paper_be,paper_floor,paper_peak,rt_pct)
                                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                               (p["id"], p["mint"], p["symbol"], p["pool"], p["leader"], S["mode"], "buying", t,
                                p["entry_ts"], p["entry_price"], p["size_usd"], lag, p["stop"], p.get("be_px"),
                                p.get("floor_px"), p["peak"], p.get("rt_pct")))
        tid = cur.lastrowid; self.con.commit(); DIRTY[0] = True
        sym = p["symbol"] or p["mint"][:6]

        def skip(why):
            self.con.execute("UPDATE trades SET status='skipped', notes=? WHERE id=?", (why, tid)); self.con.commit()
            event(self.con, "skip", f"{sym}: signal passed the filters, skipped: {why}")

        why = self.blocked(control())
        if why:
            return skip(why)
        if lag > S["max_lag"]:
            return skip(f"seen {lag:.0f} s after the engine's buy signal (limit {S['max_lag']:.0f} s)")
        if p["status"] != "open":
            return skip("the engine had already closed it")
        try:
            solpx = sol_usd()
            stake, fees = stake_for(p.get("rt_pct"), solpx)
            if stake < S["min_stake"]:
                return skip(f"costs too high: a $ {S['max_loss']:.0f} max loss allows only ${stake:.2f}")
            lam = int(stake / solpx * LAMPORTS)
            bal = self.balance()
            need = stake / solpx + S["reserve_sol"]
            low = bal is None or bal < need
            if S["mode"] == "live" and low:
                return skip(f"wallet balance {0 if bal is None else bal:.4f} SOL is below one trade plus fees ({need:.4f} SOL)")
            q = quote(SOL, p["mint"], lam, S["buy_slip_bps"])
            imp = impact(q)
            if imp > S["max_impact"]:
                return skip(f"price impact {imp:.1f}% over the {S['max_impact']:.0f}% limit")
            tx, prio = build_swap(q)
            dec = decimals(p["mint"])
            mid = prices([p["mint"]]).get(p["mint"])
            out = int(q["outAmount"])
            note = []
            if S["mode"] == "live":
                sig = sign_send(tx)
                dsol, dtok, netfee = fill_of(sig, p["mint"])
                sol_in = -dsol; out = dtok
                fee_in = netfee * solpx
            else:
                sig = None
                sol_in = lam / LAMPORTS
                fee_in = (BASE_FEE_LAMPORTS + prio) / LAMPORTS * solpx
                note.append(simulate(tx))
                if low:
                    note.append(f"live would refuse: wallet {'not set' if bal is None else f'{bal:.4f} SOL'}, needs {need:.4f} SOL")
            usd_in = sol_in * solpx
            ui = out / 10 ** dec
            entry_px = usd_in / ui if ui else None
            self.con.execute("""UPDATE trades SET status='open', stake_usd=?, sol_in=?, sol_px=?, tokens0=?, tokens=?,
                                decimals=?, mid_px=?, entry_px=?, impact_pct=?, fee_in_usd=?, buy_sig=?, buy_note=?,
                                peak=?, last_px=?, last_px_ts=? WHERE id=?""",
                             (usd_in, sol_in, solpx, str(out), str(out), dec, mid, entry_px, imp, fee_in, sig,
                              "; ".join(note) + f"; route {route_label(q)}", mid or entry_px, mid, time.time(), tid))
            self.con.commit(); DIRTY[0] = True
            event(self.con, "buy", f"{'BOUGHT' if S['mode'] == 'live' else 'would buy'} {sym}: ${usd_in:.2f} "
                                   f"({sol_in:.4f} SOL) at {entry_px:.3g}, {lag:.1f} s after the signal, "
                                   f"impact {imp:.1f}%, via {route_label(q)}" + (f" [{note[0]}]" if note else ""))
            self.leader_baseline(tid)
        except Exception as e:
            msg = str(e)[:160]
            if S["mode"] == "live":
                # a buy that may have landed is kept as 'buying' and settled from the wallet's real balance
                self.con.execute("UPDATE trades SET last_err=?, next_try=? WHERE id=?", (msg, time.time() + 5, tid))
                self.con.commit()
                event(self.con, "bad", f"{sym}: buy problem, checking the wallet: {msg}")
                self.settle_buying(tid)
            else:
                skip(f"buy failed: {msg}")

    def settle_buying(self, tid):
        """Live: a buy whose result is unknown is settled from the wallet's real token balance."""
        t = self.con.execute("SELECT * FROM trades WHERE id=?", (tid,)).fetchone()
        try:
            bal = token_balance(WALLET["address"], t["mint"])
        except Exception:
            return
        if bal > 0:
            solpx = sol_usd(); dec = decimals(t["mint"]); mid = prices([t["mint"]]).get(t["mint"])
            stake, _ = stake_for(t["rt_pct"], solpx)
            self.con.execute("""UPDATE trades SET status='open', tokens0=?, tokens=?, decimals=?, stake_usd=?, sol_px=?,
                                mid_px=?, entry_px=?, peak=?, notes='buy settled from the wallet balance' WHERE id=?""",
                             (str(bal), str(bal), dec, stake, solpx, mid, stake / (bal / 10 ** dec), mid, tid))
            event(self.con, "buy", f"{t['symbol']}: buy found in the wallet ({bal} raw tokens); now managed")
        elif time.time() - t["created_ts"] > 120:
            self.con.execute("UPDATE trades SET status='skipped', notes=? WHERE id=?", ("buy did not land: " + (t["last_err"] or ""), tid))
            event(self.con, "skip", f"{t['symbol']}: buy did not land, nothing bought")
        self.con.commit(); DIRTY[0] = True

    def leader_baseline(self, tid):
        t = self.con.execute("SELECT * FROM trades WHERE id=?", (tid,)).fetchone()
        try:
            b = token_balance(t["leader"], t["mint"])
            self.con.execute("UPDATE trades SET leader_base=?, leader_chk=? WHERE id=?", (str(b), time.time(), tid))
            self.con.commit()
        except Exception:
            pass

    # ---- selling
    def sell(self, t, frac, reason):
        """Sells frac of the bag (1.0 = all). Live: a failed sell is kept 'closing' and retried until it lands.
        Returns True when done."""
        t = dict(t)
        sym = t["symbol"] or t["mint"][:6]
        tokens = int(t["tokens"] or 0)
        if S["mode"] == "live":
            try:
                tokens = token_balance(WALLET["address"], t["mint"])
            except Exception as e:
                return self.sell_failed(t, frac, reason, f"balance read: {e}")
            if tokens == 0:
                self.finish(t, 0.0, 0.0, None, reason + " (no tokens left in the wallet)")
                return True
        amount = tokens if frac >= 0.999 else int(tokens * frac)
        if amount <= 0:
            self.finish(t, 0.0, 0.0, None, reason)
            return True
        tries = t["sell_tries"] or 0
        slip = int(min(HARD["sell_slip_bps"], S["sell_slip_bps"] * SELL_LADDER[min(tries, len(SELL_LADDER) - 1)]))
        try:
            solpx = sol_usd()
            q = quote(t["mint"], SOL, amount, slip)
            tx, prio = build_swap(q)
            if S["mode"] == "live":
                sig = sign_send(tx)
                dsol, dtok, netfee = fill_of(sig, t["mint"])
                usd = dsol * solpx + netfee * solpx       # SOL received before the network fee
                fee = netfee * solpx
                sold = -dtok
            else:
                sig = None
                usd = int(q["outAmount"]) / LAMPORTS * solpx
                fee = (BASE_FEE_LAMPORTS + prio) / LAMPORTS * solpx
                sold = amount
        except Exception as e:
            return self.sell_failed(t, frac, reason, str(e))
        if frac >= 0.999:
            self.finish(t, usd, fee, sig, reason)
        else:
            left = max(0, tokens - sold)
            self.con.execute("""UPDATE trades SET half_ts=?, half_usd=?, half_sig=?, tokens=?, fee_out_usd=COALESCE(fee_out_usd,0)+?,
                                sell_tries=0, last_err=NULL, status='open' WHERE id=?""",
                             (time.time(), usd, sig, str(left), fee, t["id"]))
            self.con.commit(); DIRTY[0] = True
            event(self.con, "sell", f"{'SOLD' if S['mode'] == 'live' else 'would sell'} half of {sym} for ${usd:.2f} ({reason})")
        return True

    def sell_failed(self, t, frac, reason, err):
        tries = (t["sell_tries"] or 0) + 1
        self.con.execute("UPDATE trades SET status='closing', sell_tries=?, next_try=?, last_err=?, exit_reason=COALESCE(exit_reason,?) WHERE id=?",
                         (tries, time.time() + SELL_RETRY_S, str(err)[:160], reason if frac >= 0.999 else None, t["id"]))
        self.con.commit(); DIRTY[0] = True
        if S["mode"] != "live" and tries >= 10:
            self.finish(t, 0.0, 0.0, None, reason + f" (dry run: no sell quote after {tries} tries, counted as $0)")
            return True
        if tries in (1, 3, 10) or tries % 25 == 0:
            event(self.con, "bad", f"{t['symbol']}: sell attempt {tries} failed, retrying every {SELL_RETRY_S} s: {str(err)[:100]}")
        return False

    def finish(self, t, usd, fee, sig, reason):
        t = dict(self.con.execute("SELECT * FROM trades WHERE id=?", (t["id"],)).fetchone())
        fee_out = (t["fee_out_usd"] or 0) + fee
        pnl = usd + (t["half_usd"] or 0) - (t["stake_usd"] or 0) - (t["fee_in_usd"] or 0) - fee_out
        self.con.execute("""UPDATE trades SET status='closed', exit_ts=?, exit_reason=?, exit_usd=?, fee_out_usd=?, sell_sig=?,
                            pnl_usd=?, tokens='0', last_err=NULL WHERE id=?""",
                         (time.time(), reason, usd, fee_out, sig, pnl, t["id"]))
        self.con.commit(); DIRTY[0] = True
        event(self.con, "sell", f"{'SOLD' if S['mode'] == 'live' else 'would sell'} {t['symbol']} for ${usd:.2f}: "
                                f"{'+' if pnl >= 0 else '-'}${abs(pnl):.2f} ({reason})")
        if S["mode"] == "live":
            try:
                close_empty_accounts()
            except Exception as e:
                log(f"closing empty token accounts failed: {e}")

    # ---- following the main account and guarding
    def mirror_exits(self, sc):
        for t in self.open_trades():
            if t["status"] == "buying":
                continue
            p = sc.execute("SELECT * FROM positions WHERE id=?", (t["paper_id"],)).fetchone()
            if not p:
                continue
            self.con.execute("UPDATE trades SET paper_stop=?, paper_peak=?, paper_be=?, paper_floor=?, paper_status=? WHERE id=?",
                             (p["stop"], p["peak"], col(p, "be_px"), col(p, "floor_px"), p["status"], t["id"]))
            if p["status"] == "closed":
                self.con.execute("UPDATE trades SET paper_exit_reason=?, paper_pnl=? WHERE id=?", (p["exit_reason"], p["pnl_usd"], t["id"]))
                self.con.commit()
                if t["status"] == "open":
                    self.sell(t, 1.0, f"exit: {p['exit_reason']}")
            elif p["part_ts"] and not t["half_ts"] and t["status"] == "open":
                self.con.commit()
                self.sell(t, C.HALF_FRAC, "half at 2x")
        self.con.commit()

    def scanner_prices(self, sc, mints):
        """While the scanner runs it already fetches these prices every ~4 s: reuse them if under 8 s old."""
        out = {}
        if sc is None:
            return out
        try:
            for m in mints:
                r = sc.execute("SELECT ts, px FROM price_ticks WHERE mint=? ORDER BY ts DESC LIMIT 1", (m,)).fetchone()
                if r and time.time() - r[0] < 8 and r[1]:
                    out[m] = r[1]
        except Exception:
            pass
        return out

    def guard(self, scanner_up, sc=None):
        opens = [t for t in self.open_trades() if t["status"] == "open"]
        if not opens:
            return
        now = time.time()
        if now - self.last_px >= PRICE_EVERY_S:
            self.last_px = now
            px = self.scanner_prices(sc, [t["mint"] for t in opens])
            miss = [t["mint"] for t in opens if t["mint"] not in px]
            if miss:
                px.update(prices(miss, {t["mint"]: t["pool"] for t in opens}))
            for t in opens:
                if t["mint"] in px:
                    pk = max(t["peak"] or 0, px[t["mint"]])
                    self.con.execute("UPDATE trades SET last_px=?, last_px_ts=?, peak=? WHERE id=?", (px[t["mint"]], now, pk, t["id"]))
            self.con.commit()
            opens = [t for t in self.open_trades() if t["status"] == "open"]
        for t in opens:
            if not t["last_px"] or now - (t["last_px_ts"] or 0) > 60:
                continue
            # 1) own $5 max-loss line, from this trade's own buy: what selling now would bring back (live price,
            #    minus about half the round-trip cost for the sell side, minus the sell's network fee)
            ui = int(t["tokens"] or 0) / 10 ** (t["decimals"] or 0)
            sell_cost = (t["rt_pct"] or 3.0) / 200
            value = ui * t["last_px"] * (1 - sell_cost)
            est = value + (t["half_usd"] or 0) - (t["stake_usd"] or 0) - (t["fee_in_usd"] or 0) - (t["fee_in_usd"] or 0)
            if est <= -S["max_loss"] and not t["half_ts"]:
                self.sell(t, 1.0, f"Radium's own ${S['max_loss']:.0f} max-loss line (about {est:+.2f} on live price)")
                continue
            # 2) main-account exits on live prices (paper prices: same feed, same units)
            entry = t["paper_entry_px"]
            if not entry:
                continue
            pk = max(t["paper_peak"] or 0, t["peak"] or 0)
            stop = stop_for(FLOOR_SPEC, entry, pk, max(t["paper_stop"] or 0, t["gstop"] or 0), t["paper_be"], t["paper_floor"])
            self.con.execute("UPDATE trades SET gstop=? WHERE id=?", (stop, t["id"]))
            hit = None
            if t["last_px"] <= stop:
                hit = f"stop hit on live price ({t['last_px'] / entry - 1:+.0%} vs paper entry)"
            elif not t["half_ts"] and t["last_px"] >= entry * C.HALF_AT:
                hit = "half"
            elif now - (t["paper_entry_ts"] or now) > C.TIME_STOP_S:
                hit = "24 h time stop"
            if hit and scanner_up:
                # the main account normally does this itself; only step in if it hasn't after MIRROR_GRACE_S
                if not t["below_since"]:
                    self.con.execute("UPDATE trades SET below_since=? WHERE id=?", (now, t["id"])); self.con.commit()
                    continue
                if now - t["below_since"] < MIRROR_GRACE_S:
                    continue
                hit = hit if hit == "half" else hit + ", engine slow to act"
            elif not hit and t["below_since"]:
                self.con.execute("UPDATE trades SET below_since=NULL WHERE id=?", (t["id"],))
            if hit == "half":
                self.sell(t, C.HALF_FRAC, "2x on live price (Radium's own guard)")
            elif hit:
                self.sell(t, 1.0, hit + " (Radium's own guard)")
            elif not scanner_up and t["leader"] and now - (t["leader_chk"] or 0) >= LEADER_EVERY_S:
                # 3) copied-wallet sell, checked on chain while the scanner (which normally sees it) is down
                try:
                    b = token_balance(t["leader"], t["mint"])
                    base = int(t["leader_base"] or 0)
                    self.con.execute("UPDATE trades SET leader_chk=? WHERE id=?", (now, t["id"]))
                    if base > 0 and b <= base * (1 - C.MIRROR_SELL_FRAC):
                        self.sell(t, 1.0, "copied wallet sold (Radium's own on-chain check)")
                except Exception:
                    self.con.execute("UPDATE trades SET leader_chk=? WHERE id=?", (now, t["id"]))
        self.con.commit()

    def retry_sells(self):
        for t in self.con.execute("SELECT * FROM trades WHERE status='closing' AND COALESCE(next_try,0)<=?", (time.time(),)).fetchall():
            self.sell(t, 1.0, t["exit_reason"] or "retrying a sell")
        if S["mode"] == "live":
            for t in self.con.execute("SELECT * FROM trades WHERE status='buying' AND COALESCE(next_try,0)<=?", (time.time(),)).fetchall():
                self.settle_buying(t["id"])

    def sell_everything(self, why):
        for t in self.open_trades():
            if t["status"] == "open" or (t["status"] == "closing" and (t["next_try"] or 0) <= time.time()):
                self.sell(t, 1.0, why)

    def reconcile(self):
        """Live, on taking the lock: the wallet's real token balances win over the saved state."""
        if S["mode"] != "live" or not WALLET["address"]:
            return
        try:
            held = {}
            for _, m, amt, _ in token_accounts(WALLET["address"]):
                held[m] = held.get(m, 0) + amt
        except Exception as e:
            event(self.con, "bad", f"startup wallet check failed, will retry: {e}"); return
        mine = set()
        for t in self.open_trades():
            mine.add(t["mint"])
            if t["status"] == "buying":
                self.settle_buying(t["id"]); continue
            real = held.get(t["mint"], 0)
            if real == 0:
                self.finish(t, 0.0, 0.0, None, "not in the wallet at startup (sold earlier)")
            elif str(real) != t["tokens"]:
                self.con.execute("UPDATE trades SET tokens=? WHERE id=?", (str(real), t["id"]))
        other = [m for m, a in held.items() if a > 0 and m not in mine and m != SOL]
        meta_set(self.con, "unmanaged_tokens", len(other))
        self.con.commit()
        event(self.con, "info", f"startup wallet check: {len(mine)} open trade(s) resumed"
                                + (f", {len(other)} other token(s) in the wallet left alone" if other else ""))

    # ---- dashboard
    def engine_info(self):
        """Health of its own signal engine, for the page: wallets followed, feed, signals in the last hour."""
        if not os.path.exists(SCANNER_DB):
            return None
        try:
            sc = sqlite3.connect(f"file:{SCANNER_DB}?mode=ro", uri=True, timeout=5)
            try:
                t = time.time()
                hb = sc.execute("SELECT MAX(ts) FROM heartbeat").fetchone()[0]
                ws = [r[0] or "" for r in sc.execute("SELECT name FROM wallets WHERE active=1 ORDER BY name")]
                n1 = sc.execute("SELECT COUNT(*), SUM(status='survivor') FROM signals WHERE detected_ts>?", (t - 3600,)).fetchone()
                why = sc.execute("""SELECT killed_by, COUNT(*) FROM signals WHERE detected_ts>? AND killed_by IS NOT NULL
                                    GROUP BY killed_by ORDER BY 2 DESC LIMIT 4""", (t - 3600,)).fetchall()
                lag = sc.execute("""SELECT detect_lag_s FROM signals WHERE detected_ts>? AND detect_lag_s IS NOT NULL
                                    ORDER BY detect_lag_s""", (t - 86400,)).fetchall()
                last = sc.execute("""SELECT symbol, detected_ts, status, killed_by FROM signals ORDER BY id DESC LIMIT 12""").fetchall()
                try:
                    feed = sc.execute("SELECT value FROM meta WHERE key='feed'").fetchone()
                    feed = json.loads(feed[0]) if feed else None
                except Exception:
                    feed = None
            finally:
                sc.close()
            return {"hb": hb, "up": bool(hb and t - hb < SCANNER_FRESH_S), "wallets": ws,
                    "sig_1h": n1[0] or 0, "pass_1h": n1[1] or 0, "why_1h": [[k, c] for k, c in why],
                    "lag_med": lag[len(lag) // 2][0] if lag else None, "lag_n": len(lag),
                    "feed": ({"connected": bool(feed.get("connected")), "helius": "helius" in str(feed.get("url") or ""),
                              "reconnects": feed.get("reconnects"), "uptime_s": feed.get("uptime_s"), "ts": feed.get("ts")}
                             if isinstance(feed, dict) else None),
                    "last": [{"coin": a or "?", "t": b, "st": c, "why": d} for a, b, c, d in last]}
        except Exception as e:
            return {"err": type(e).__name__}

    def publish_page(self):
        """Its own page (rage-radium-bot repo) reads radium.enc from the radium-data branch: the status sealed with
        RADIUM_DASH_KEY, so only someone with the page link's key can read it. Runs in a thread, never blocks."""
        remote = os.environ.get("RADIUM_PAGE_REMOTE") or (f"https://x-access-token:{TOKEN}@github.com/{REPO}.git"
                                                           if TOKEN and REPO and not os.environ.get("RADIUM_TEST_RUNS") else "")
        if not os.environ.get("RADIUM_DASH_KEY") or not remote:
            return
        if getattr(self, "_pub_busy", False):
            return
        self._pub_busy = True
        def go():
            try:
                from cryptography.hazmat.primitives.ciphers.aead import AESGCM
                k = os.environ["RADIUM_DASH_KEY"].strip()
                aes = AESGCM(base64.urlsafe_b64decode(k + "=" * (-len(k) % 4)))
                with open(STATUS, "rb") as f:
                    raw = f.read()
                n = os.urandom(12)
                blob = base64.b64encode(n + aes.encrypt(n, gzip.compress(raw, 6), None))
                d = os.path.join(RSTATE, "pagerepo")
                def g(*a, data=None):
                    return subprocess.run(["git", *a], cwd=d, input=data, capture_output=True, timeout=60)
                if not os.path.isdir(os.path.join(d, ".git")):
                    os.makedirs(d, exist_ok=True)
                    g("init", "-q"); g("config", "user.name", "rage-radium-bot")
                    g("config", "user.email", "rage-radium-bot@users.noreply.github.com")
                    g("remote", "add", "origin", remote)
                b = g("hash-object", "-w", "--stdin", data=blob).stdout.decode().strip()
                tree = g("mktree", data=f"100644 blob {b}\tradium.enc\n".encode()).stdout.decode().strip()
                c = g("commit-tree", tree, "-m", "Rage Radium Bot page data").stdout.decode().strip()
                r = g("push", "-q", "--force", "origin", f"{c}:refs/heads/radium-data")
                if r.returncode != 0:
                    log("page publish failed")
            except Exception as e:
                log(f"page publish error {type(e).__name__}")
            finally:
                self._pub_busy = False
        threading.Thread(target=go, name="page", daemon=True).start()

    def write_status(self):
        if not self.con:
            return
        self.last_status = time.time()
        ctl = control()
        rows = [dict(r) for r in self.con.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 60")]
        def tr(r):
            ui = int(r["tokens"] or 0) / 10 ** (r["decimals"] or 0) if r["decimals"] is not None else 0
            val = ui * (r["last_px"] or 0)
            return {"coin": r["symbol"] or (r["mint"] or "")[:6], "status": r["status"], "mode": r["mode"],
                    "t": r["created_ts"], "lag": r["lag_s"], "stake": r["stake_usd"], "entry": r["entry_px"],
                    "paper_entry": r["paper_entry_px"], "impact": r["impact_pct"], "rt": r["rt_pct"],
                    "now_px": r["last_px"], "value": val, "half_usd": r["half_usd"], "exit_usd": r["exit_usd"],
                    "exit_ts": r["exit_ts"], "reason": r["exit_reason"], "pnl": r["pnl_usd"], "paper_pnl": r["paper_pnl"],
                    "paper_reason": r["paper_exit_reason"], "paper_size": r["paper_size"], "note": r["notes"] or r["buy_note"],
                    "err": r["last_err"], "tries": r["sell_tries"], "stop": r["gstop"] or r["paper_stop"]}
        closed = [r for r in rows if r["status"] == "closed"]
        allc = self.con.execute("SELECT COUNT(*), COALESCE(SUM(pnl_usd),0), COALESCE(SUM(paper_pnl),0), SUM(pnl_usd>0) FROM trades WHERE status='closed' AND mode=?", (S["mode"],)).fetchone()
        nskip = self.con.execute("SELECT COUNT(*) FROM trades WHERE status='skipped'").fetchone()[0]
        bal = self.bal["sol"]
        solpx = _J["sol"][1] or None
        st = {"v": 1, "ts": time.time(), "mode": S["mode"], "role": self.role, "run": RUN_ID, "state": self.mode_now,
              "owner": bool(OWNER["ok"]), "since": meta_get(self.con, "since"),
              "wallet_set": bool(WALLET["address"]), "key_set": bool(WALLET.get("key_present")),
              "key_matches": WALLET.get("key_ok"), "balance_sol": bal, "balance_usd": (bal * solpx) if (bal is not None and solpx) else None,
              "balance_err": self.bal["err"], "sol_px": solpx,
              "control": {"pause": bool(ctl.get("pause")), "sell_all_and_stop": bool(ctl.get("sell_all_and_stop")), "stale": ctl.get("stale")},
              "blocked": self.blocked(ctl),
              "limits": {k: S[k] for k in ("max_loss", "max_stake", "min_stake", "stop_pct", "max_open", "daily_loss", "total_loss",
                                           "buy_slip_bps", "sell_slip_bps", "max_impact", "max_lag", "priority_lamports", "reserve_sol")},
              "today": self.realised(lagos_day_start()), "stats": {"closed": allc[0], "pnl": allc[1], "paper_pnl": allc[2], "wins": allc[3] or 0, "skipped": nskip},
              "open": [tr(r) for r in rows if r["status"] in ("buying", "open", "closing")],
              "closed": [tr(r) for r in closed[:30]],
              "skipped": [tr(r) for r in rows if r["status"] == "skipped"][:15],
              "events": [dict(e) for e in self.con.execute("SELECT ts,kind,msg FROM events ORDER BY id DESC LIMIT 40")],
              "unmanaged_tokens": meta_get(self.con, "unmanaged_tokens", 0),
              "engine": self.engine_info()}
        tmp = STATUS + ".tmp"
        with open(tmp, "w") as f:
            f.write(scrub(json.dumps(st, default=str)))
        os.replace(tmp, STATUS)

    # ---- main loop
    def run(self):
        if S["mode"] == "off":
            log("RADIUM_MODE=off: not starting"); return
        if not self.acquire():
            return
        if S["mode"] == "live" and os.environ.get("LIVE_MODE", "").strip().lower() == "live":
            event(self.con, "bad", "the older live executor (LIVE_MODE) is also set to live: refusing to trade live, staying in dry-run")
            S["mode"] = "dry-run"; WALLET["kp"] = None
        if S["mode"] == "live":
            if WALLET["kp"] is None:
                event(self.con, "bad", "live mode asked for but the key is missing or doesn't match the address: staying in dry-run")
                S["mode"] = "dry-run"
            else:
                self.reconcile()
        self.balance()
        while True:
            now = time.time()
            if not self.check_owner():
                self.write_status(); return
            want = (OWNER["lock"] or {}).get("want")
            if want and self.role == "bridge" and str(want.get("run")) != RUN_ID:
                self.handover(f"{want['role']} run is up"); return
            if os.path.exists(RELEASE):
                # run ending: hand to the bridge. If the bridge hasn't asked yet and trades are open, keep guarding
                # them for up to 2 minutes while it starts, rather than leave them with nobody.
                self.release_seen = self.release_seen or now
                if want or not self.open_trades() or now - self.release_seen > 120:
                    self.handover("run ending"); return
            if self.role == "bridge" and now - self.started > BRIDGE_MAX_S and not self.next_bridge_sent:
                self.next_bridge_sent = dispatch("radium-bridge.yml")
            try:
                ctl = control()
                sc, hb = self.scanner()
                up = bool(hb and now - hb < SCANNER_FRESH_S)
                self.mode_now = "engine" if up else ("guard (engine restarting)" if sc else "guard (bridge)")
                if ctl.get("sell_all_and_stop"):
                    self.sell_everything("phone switch: sell everything and stop")
                elif sc is not None:
                    self.mirror_exits(sc)       # the engine's closes/halves are acted on whenever its data is here
                    if up:
                        self.mirror_buys(sc)    # new buys only while the engine is actually running
                self.guard(up, sc)
                self.retry_sells()
                if sc:
                    sc.close()
            except Exception as e:
                log(f"loop error {e}\n{traceback.format_exc()}")
                try:
                    self.con.rollback()
                except Exception:
                    pass
            if DIRTY[0] and now - self.last_save > 3 or now - self.last_save > BEAT_EVERY_S:
                if not self.save():
                    if not self.check_owner():
                        return
            if now - self.last_status > 10:
                self.balance()
                self.write_status()
                if now - getattr(self, "last_pub", 0) > 60:
                    self.last_pub = now
                    self.publish_page()
            time.sleep(1)


def already_running():
    try:
        with open(PIDFILE) as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return b"radium.py" in f.read()
    except Exception:
        return False


def main():
    role = "primary"
    if "--role" in sys.argv:
        role = sys.argv[sys.argv.index("--role") + 1]
    os.makedirs(RSTATE, exist_ok=True)
    if already_running():
        print("radium daemon already running"); return
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))
    load_wallet()
    log(f"start: role {role}, mode {S['mode']}, run {RUN_ID}, wallet {'set' if WALLET['address'] else 'not set'}, "
        f"key {'set' if WALLET['key_present'] else 'not set'}")
    signal.signal(signal.SIGTERM, lambda *a: (_ for _ in ()).throw(KeyboardInterrupt()))
    bot = Bot(role)
    try:
        bot.run()
    except KeyboardInterrupt:
        log("stopped by SIGTERM")
        if OWNER["ok"] and bot.con:
            try:
                bot.save(note="stopped")
            except Exception:
                pass
    finally:
        try:
            os.remove(PIDFILE)
        except Exception:
            pass
    log("exit")


if __name__ == "__main__":
    main()
