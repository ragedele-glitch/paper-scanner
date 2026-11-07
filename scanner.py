#!/usr/bin/env python3
"""Paper-mode Solana memecoin copy-trade scanner.

Scout   -> follows profitable wallets from the public Kolscan leaderboard and spots their new coin buys
           from a live read-only websocket feed of their transactions (stream.py), with a slow holdings
           re-read every 10 min as a safety net (and as the only source whenever the feed is down).
Filter  -> kills any coin breaking a hard rule (dev >5%, liquidity <$25k, top-10 >35%, bundled, +100% in 1h,
           smart wallet sold early) plus basic safety checks (mint/freeze authority still live).
Sniper  -> paper-buys survivors at the moment the scanner saw the smart wallet's buy (realistic, headline accounts
           11 and 10), and separately 5 s after the smart wallet (old optimistic test, accounts 1 and 0),
           both with slippage and fees.
Guard   -> stop set at entry, moved to breakeven, then trailed tighter as gains grow; mirror-sell; 24 h time stop.

PAPER MODE by default: no wallet, no private keys, nothing is ever signed or sent.
Live mode (live.py) is a separate, switched-off add-on; see live.py and LIVE.md.
"""
import os, base64, calendar, gzip, json, logging, logging.handlers, math, queue, re, sqlite3, struct, sys, threading, time, traceback
from concurrent.futures import ThreadPoolExecutor
import urllib.request, urllib.error
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as C
import fills
import holders
import walletcard
import live   # live mode: switched off unless LIVE_MODE is set; with it off every hook returns at once
import stream as streammod

STREAM = None          # the live wallet feed (stream.WalletStream), started in main()
SEEN_SIGS = deque(maxlen=20000); SEEN_SIG_SET = set()
SEEN_BUYS = {}         # (wallet, mint) -> time a first buy was taken from the feed (stops the safety-net poll repeating it)
PENDING = []           # feed buys of coins DexScreener hasn't listed yet, retried for up to DEX_RETRY_S
CANDLES = {}           # (pool, mint) -> (fetched_at, since, candles): one GeckoTerminal call serves every account


def live_hook(fn, *a):
    """Live mode can never break paper mode: any error in a live hook is logged and ignored."""
    try:
        fn(*a)
    except Exception as e:
        logging.getLogger("scanner").error("live hook error %s", e)

log = logging.getLogger("scanner")

# ----------------------------------------------------------------------------- helpers
B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58(b):
    n = int.from_bytes(b, "big"); s = ""
    while n:
        n, r = divmod(n, 58); s = B58[r] + s
    return "1" * (len(b) - len(b.lstrip(b"\0"))) + s


class Limiter:
    def __init__(self, per_min):
        self.per_min = per_min; self.t = deque(); self.lock = threading.Lock()

    def wait(self):   # thread-safe: the live feed reads transactions on several threads at once
        while True:
            with self.lock:
                now = time.time()
                while self.t and now - self.t[0] > 60:
                    self.t.popleft()
                if len(self.t) < self.per_min:
                    self.t.append(now); return
                delay = 60 - (now - self.t[0]) + 0.2
            time.sleep(max(0.05, delay))

    def free(self):
        with self.lock:
            now = time.time()
            while self.t and now - self.t[0] > 60:
                self.t.popleft()
            return self.per_min - len(self.t)


LIM = {"rpc": Limiter(200), "gecko": Limiter(25), "rug": Limiter(12), "dex": Limiter(200), "web": Limiter(5)}
STATS = {"rpc_bytes": 0}
RPC_UA = "memecoin-paper-scanner/1.0 (read-only, python-urllib)"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"


def http(url, data=None, kind="web", headers=None, timeout=30, tries=3, raw=False):
    # The public Solana RPC now rejects requests that claim to be a browser (HTTP 403 since Nov 2),
    # so RPC calls identify honestly as a script; web pages keep the browser UA.
    h = {"User-Agent": RPC_UA if kind == "rpc" else UA, "Accept": "application/json", "Accept-Encoding": "gzip"}
    if headers:
        h.update(headers)
    for i in range(tries):
        LIM[kind].wait()
        try:
            req = urllib.request.Request(url, data=data, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read()
                if r.headers.get("Content-Encoding") == "gzip":
                    body = gzip.decompress(body)
            if kind == "rpc":
                STATS["rpc_bytes"] += len(body)
            return body if raw else json.loads(body)
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and i < tries - 1:
                time.sleep(3 * (i + 1) * (3 if e.code == 429 else 1)); continue
            if e.code == 404:
                return None
            raise
        except Exception:
            if i < tries - 1:
                time.sleep(2 * (i + 1)); continue
            raise


def rpc(method, params):
    j = http(C.RPC_URL, json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
             kind="rpc", headers={"Content-Type": "application/json"}, timeout=60)
    if j is None or "error" in j:
        raise RuntimeError(f"rpc {method} error: {j and j.get('error')}")
    return j["result"]


def now():
    return time.time()


# ----------------------------------------------------------------------------- database
SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS wallets(address TEXT PRIMARY KEY, name TEXT, first_seen REAL, last_seen REAL,
  last_rank INT, pnl_sol REAL, pnl_usd REAL, wins INT, losses INT, n_accounts INT, poll_s INT, last_poll REAL, active INT DEFAULT 1);
CREATE TABLE IF NOT EXISTS snapshots(address TEXT PRIMARY KEY, ts REAL, holdings TEXT);
CREATE TABLE IF NOT EXISTS signals(id INTEGER PRIMARY KEY, detected_ts REAL, wallet TEXT, wallet_name TEXT, mint TEXT,
  symbol TEXT, name TEXT, ata TEXT, leader_buy_ts REAL, leader_buy_sig TEXT, leader_post_amt REAL, leader_usd REAL,
  detect_lag_s REAL, pool TEXT, dex TEXT, price_detect REAL, liq_usd REAL, h1_pct REAL, mcap REAL, pair_age_min REAL,
  dev_pct REAL, top10_pct REAL, insider_pct REAL, graph_insiders INT, mint_auth INT, freeze_auth INT, rug_score REAL,
  killed_by TEXT, status TEXT, notes TEXT, p1h REAL, p6h REAL, p24h REAL, max24h REAL);
CREATE INDEX IF NOT EXISTS sig_mint ON signals(mint);
CREATE TABLE IF NOT EXISTS positions(id INTEGER PRIMARY KEY, signal_id INT, account INT, mint TEXT, symbol TEXT, pool TEXT,
  leader TEXT, entry_ts REAL, entry_price REAL, entry_src TEXT, size_usd REAL, qty REAL, stop REAL, peak REAL,
  last_candle_ts REAL, last_price REAL, last_update REAL, status TEXT, exit_ts REAL, exit_price REAL, exit_reason TEXT,
  proceeds REAL, pnl_usd REAL, pnl_pct REAL, stop_log TEXT);
CREATE TABLE IF NOT EXISTS leader_sells(wallet TEXT, mint TEXT, ts REAL, sold_frac REAL, PRIMARY KEY(wallet, mint));
CREATE TABLE IF NOT EXISTS equity(ts REAL PRIMARY KEY, cash REAL, open_value REAL, equity REAL, n_open INT);
CREATE TABLE IF NOT EXISTS heartbeat(ts REAL PRIMARY KEY, polled INT, rpc_mb REAL, note TEXT);
CREATE TABLE IF NOT EXISTS equity_real(ts REAL PRIMARY KEY, cash REAL, open_value REAL, equity REAL, n_open INT);
CREATE TABLE IF NOT EXISTS equity_hf(ts REAL PRIMARY KEY, cash REAL, open_value REAL, equity REAL, n_open INT);
CREATE TABLE IF NOT EXISTS paper_resets(reset_id TEXT, ts REAL, account INT, session_since REAL, cash_before REAL,
  open_value_before REAL, equity_before REAL, n_open INT, closed INT, wins INT, realised REAL, cash_after REAL,
  PRIMARY KEY(reset_id, account));
"""
MIGRATIONS = [
    "ALTER TABLE signals ADD COLUMN detect_src TEXT",      # 'stream' (live feed) or 'poll' (holdings re-read)
    "ALTER TABLE leader_sells ADD COLUMN det_ts REAL",     # when the scanner saw the smart wallet's sell
    "ALTER TABLE heartbeat ADD COLUMN ws INT",             # 1 = live feed connected at this check-in
    "ALTER TABLE heartbeat ADD COLUMN stream_n INT",       # wallet transactions read from the feed since last check-in
    "ALTER TABLE wallets ADD COLUMN card_status TEXT",     # wallet scorecard: pinned / keep / new / bench / removed
    "ALTER TABLE wallets ADD COLUMN card_total REAL",      # checklist score (out of 7)
    "ALTER TABLE wallets ADD COLUMN card_json TEXT",       # scores per item and the facts behind them
    "CREATE TABLE IF NOT EXISTS wallet_scores(ts REAL, address TEXT, status TEXT, total REAL, items INT, PRIMARY KEY(ts, address))",
] + [f"ALTER TABLE positions ADD COLUMN {c}" for c in (
    "fill_model TEXT",     # 'live' = live-like fills (fills.py); NULL = flat 3% slippage model
    "leader_px REAL",      # price the copied wallet got (first trade at its buy time): the 'ideal' entry
    "seen_px REAL",        # price when the scanner saw the buy
    "r_buy REAL", "r_sell_est REAL", "r_sell REAL",   # fill price / mid, from real Jupiter quotes for this size
    "rt_pct REAL",         # round-trip loss % (buy then sell back) at entry, for this size
    "q_src_in TEXT", "q_src_out TEXT", "route TEXT",
    "fee_in REAL", "fee_out REAL",                    # network: base + priority + Jito tip, in dollars
    "liq_in REAL", "liq_out REAL", "sellable INT",
    "ideal_pnl REAL", "gap_delay REAL", "gap_impact REAL", "gap_botfee REAL", "gap_netfee REAL",
    "rules TEXT",          # 'v3' = $5 max-loss stop + breakeven at +50% + half at 2x + widening trail; NULL = original
    "be_px REAL",          # price where the trade breaks even after all costs
    "exit_trigger_px REAL", "exit_fill_src TEXT",   # stop level vs the price a live sell would really have got
    "part_ts REAL", "part_px REAL", "part_qty REAL", "part_proceeds REAL", "part_r REAL", "part_fee REAL",
    "alts TEXT", "alts_open INT",   # the same trade under the other exit rules, for comparison
    "price_src TEXT",     # where the bot's last price came from: 'candle', 'jupiter' or 'dexscreener' (live quote)
    "price_ts REAL")      # when that price was true (candle end or quote time), so the page can show its real age
] + [f"ALTER TABLE signals ADD COLUMN {c}" for c in (
    "holders INT", "transfer_fee_pct REAL", "lp_locked_pct REAL", "token_program TEXT",
    "rt_pct REAL", "fill_note TEXT",
    "hf_top10 REAL", "hf_top1 REAL",   # top-10 / top-1 holders' % of supply, leaving out the curve and the copied wallet
    "hf_block INT",                    # 1 = the holder-filter test account skipped it; 0 = took it; NULL = not checked
    "hf_info TEXT",                    # holder check details (JSON) or why it failed
    "chain_info TEXT")]                # creator and early-buyer numbers (JSON), filled in a few seconds later


def migrate(con):
    con.executescript(SCHEMA)
    for m in MIGRATIONS:
        try:
            con.execute(m)
        except sqlite3.OperationalError:
            pass
    con.commit()


def db():
    con = sqlite3.connect(C.DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def meta_get(con, k, default=None):
    r = con.execute("SELECT value FROM meta WHERE key=?", (k,)).fetchone()
    return json.loads(r[0]) if r else default


def meta_set(con, k, v):
    con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (k, json.dumps(v)))


# ----------------------------------------------------------------------------- Scout: wallet list
def refresh_board(con):
    html = http(C.KOLSCAN_URL, kind="web", raw=True, headers={"Accept": "text/html"}).decode("utf-8", "ignore")
    blocks = html.split("leaderboard_leaderboardUser__")[1:]
    rows = []
    for rank, b in enumerate(blocks, 1):
        m = re.search(r'href="/account/([1-9A-HJ-NP-Za-km-z]{32,44})\?timeframe=', b)
        if not m:
            continue
        name = re.search(r'font-weight:550">([^<]*)</h1>', b)
        wl = re.search(r'margin-right:2px">(\d+)</p>/<p[^>]*>(\d+)</p>', b)
        pnl = re.search(r'<h1>([+-]?[\d,.]+)<!-- --> <!-- -->Sol</h1><h1>\(<!-- -->(-?)\$?(-?)([\d,.]+)', b)
        rows.append(dict(addr=m.group(1), name=name.group(1) if name else "", rank=rank,
                         wins=int(wl.group(1)) if wl else None, losses=int(wl.group(2)) if wl else None,
                         pnl_sol=float(pnl.group(1).replace(",", "")) if pnl else None,
                         pnl_usd=(-1 if (pnl and (pnl.group(2) or pnl.group(3))) else 1) * float(pnl.group(4).replace(",", "")) if pnl else None))
    seen = set(); t = now()
    for r in rows:
        if r["addr"] in seen:
            continue
        seen.add(r["addr"])
        con.execute("""INSERT INTO wallets(address,name,first_seen,last_seen,last_rank,pnl_sol,pnl_usd,wins,losses,poll_s,last_poll)
            VALUES(?,?,?,?,?,?,?,?,?,60,0) ON CONFLICT(address) DO UPDATE SET name=excluded.name,last_seen=excluded.last_seen,
            last_rank=excluded.last_rank,pnl_sol=excluded.pnl_sol,pnl_usd=excluded.pnl_usd,wins=excluded.wins,losses=excluded.losses""",
                    (r["addr"], r["name"], t, t, r["rank"], r["pnl_sol"], r["pnl_usd"], r["wins"], r["losses"]))
    cand = follow_set(con, t)
    con.execute("UPDATE wallets SET active=0")
    for a in cand:
        con.execute("UPDATE wallets SET active=1 WHERE address=?", (a,))
    meta_set(con, "board_ts", t)
    con.commit()
    log.info("board refreshed: %d rows parsed, %d wallets followed", len(rows), len(cand))


def follow_set(con, t):
    """Wallets to follow, from Rage's checklist scorecard (walletcard.py).
    Pinned (Stigman, Fox) always; then wallets that made the bot money (seen on the board in the last 14 days);
    then wallets on the board in the last BOARD_KEEP_DAYS with positive profit that score 'keep' or are too new to
    score, best score first. Removed and benched wallets are never followed. If scoring fails, the old rule is used
    (still without removed wallets)."""
    old = [r[0] for r in con.execute("""SELECT address FROM wallets WHERE last_seen>? AND COALESCE(pnl_sol,0)>0
                          ORDER BY last_seen DESC, last_rank ASC""", (t - C.BOARD_KEEP_DAYS * 86400,))
           if r[0] not in C.REMOVED_WALLETS]
    try:
        board = walletcard.kolscan_boards()
        # new candidates from the 7- and 30-day boards: profitable on both
        # also: on the 30-day board only, profitable, with a 50%+ win rate over 20+ trades (Rage, Nov 7)
        for a, b in board.items():
            m = b.get(30, {})
            strong30 = m.get("sol", 0) > 0 and m.get("w", 0) + m.get("l", 0) >= 20 and m["w"] / (m["w"] + m["l"]) >= 0.5
            if (b.get(7, {}).get("sol", 0) > 0 and m.get("sol", 0) > 0) or (7 not in b and strong30):
                con.execute("""INSERT INTO wallets(address,name,first_seen,last_seen,last_rank,pnl_sol,poll_s,last_poll)
                    VALUES(?,?,?,?,99,?,60,0) ON CONFLICT(address) DO UPDATE SET last_seen=MAX(last_seen, excluded.last_seen),
                    pnl_sol=COALESCE(pnl_sol, excluded.pnl_sol)""", (a, b.get("name", ""), t, t, b.get(7, m)["sol"]))
        cards = walletcard.score_all(con, board, t)
        seen = {r[0]: r[1] for r in con.execute("SELECT address,last_seen FROM wallets")}
        pos = {r[0]: r[1] for r in con.execute("SELECT address,COALESCE(pnl_sol,0) FROM wallets")}
        for a, c in cards.items():
            c["status"] = walletcard.status(a, c)
            if a in seen:
                con.execute("UPDATE wallets SET card_status=?, card_total=?, card_json=? WHERE address=?",
                            (c["status"], c["total"], json.dumps({"score": c["score"], "items": c["items"], "facts": c["facts"]}), a))
                con.execute("INSERT OR REPLACE INTO wallet_scores VALUES(?,?,?,?,?)", (round(t), a, c["status"], c["total"], c["items"]))
        pinned = [a for a in C.PINNED_WALLETS]
        proven = sorted((a for a, c in cards.items() if c["status"] == "keep" and walletcard.proven(c)
                         and seen.get(a, 0) > t - 14 * 86400 and a not in pinned), key=lambda a: -cards[a]["total"])
        rest = sorted((a for a, c in cards.items() if c["status"] in ("keep", "new") and a not in pinned and a not in proven
                       and seen.get(a, 0) > t - C.BOARD_KEEP_DAYS * 86400 and pos.get(a, 0) > 0),
                      key=lambda a: (cards[a]["status"] != "keep", -cards[a]["total"]))
        out = [a for a in pinned if a in seen] + proven + rest
        return out[:C.MAX_WALLETS + len(pinned)]
    except Exception as e:
        log.warning("wallet scorecard failed (%s), following the board as before", e)
        return old[:C.MAX_WALLETS]


def snapshot(addr):
    """Current token holdings of a wallet: {mint: [amount_raw, biggest_token_account]} plus account count."""
    hold = {}; n = 0
    for prog in C.TOKEN_PROGRAMS:
        res = rpc("getTokenAccountsByOwner", [addr, {"programId": prog},
                  {"encoding": "base64", "dataSlice": {"offset": 0, "length": 72}, "commitment": "confirmed"}])
        for a in res["value"]:
            n += 1
            d = base64.b64decode(a["account"]["data"][0])
            if len(d) < 72:
                continue
            amt = struct.unpack("<Q", d[64:72])[0]
            if amt == 0:
                continue
            mint = b58(d[:32])
            cur = hold.get(mint)
            if cur is None:
                hold[mint] = [amt, a["pubkey"]]
            else:
                cur[0] += amt
    return hold, n


def token_delta(tx, owner, mint):
    m = tx["meta"]; pre = post = 0.0; dec = None
    for b in m.get("preTokenBalances") or []:
        if b.get("owner") == owner and b.get("mint") == mint:
            pre += float(b["uiTokenAmount"]["amount"]); dec = b["uiTokenAmount"]["decimals"]
    for b in m.get("postTokenBalances") or []:
        if b.get("owner") == owner and b.get("mint") == mint:
            post += float(b["uiTokenAmount"]["amount"]); dec = b["uiTokenAmount"]["decimals"]
    return pre, post, dec


def paid_something(tx, owner):
    """True if the owner spent SOL/wSOL/USDC/USDT in this tx (a swap), not just received tokens."""
    m = tx["meta"]
    keys = [k["pubkey"] if isinstance(k, dict) else k for k in tx["transaction"]["message"]["accountKeys"]]
    if owner in keys:
        i = keys.index(owner)
        if (m["preBalances"][i] - m["postBalances"][i]) > 0.005e9:
            return True
    for mint in ("So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                 "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"):
        pre, post, _ = token_delta(tx, owner, mint)
        if pre - post > 0:
            return True
    return False


def ata_history(ata, since_ts, limit=25):
    sigs = rpc("getSignaturesForAddress", [ata, {"limit": limit, "commitment": "confirmed"}])
    return sorted([s for s in sigs if not s.get("err") and (s.get("blockTime") or 0) >= since_ts - 10],
                  key=lambda s: s["blockTime"])


def get_tx(sig):
    return rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1, "commitment": "confirmed"}])


# ----------------------------------------------------------------------------- market data
def dex_pairs(mints):
    """{mint: [pairs]} from DexScreener, 30 per call."""
    out = {}
    mints = list(mints)
    for i in range(0, len(mints), 30):
        chunk = mints[i:i + 30]
        try:
            d = http(C.DEX_TOKENS + ",".join(chunk), kind="dex") or []
        except Exception as e:
            log.warning("dexscreener error %s", e); continue
        for p in d:
            if p.get("chainId") != "solana":
                continue
            out.setdefault(p["baseToken"]["address"], []).append(p)
    return out


def top_pair(pairs):
    if not pairs:
        return None
    return max(pairs, key=lambda p: ((p.get("liquidity") or {}).get("usd") or 0, p.get("volume", {}).get("h24") or 0))


def gecko_candles(pool, mint, since_ts):
    """1-minute OHLCV (usd, for this token) with start time >= since_ts, ascending. Paginates back up to ~3 days."""
    out = []; before = int(now()) + 60
    for _ in range(5):
        d = http(f"{C.GECKO}/pools/{pool}/ohlcv/minute?aggregate=1&limit=1000&currency=usd&token={mint}&before_timestamp={before}",
                 kind="gecko")
        lst = (((d or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
        if not lst:
            break
        out.extend(lst)
        oldest = min(c[0] for c in lst)
        if oldest <= since_ts or len(lst) < 1000:
            break
        before = oldest
    cs = sorted({int(c[0]): c for c in out if c[0] >= since_ts}.values(), key=lambda c: c[0])
    return [(int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4])) for c in cs]


def gecko_prices_at(pool, mint, tss):
    """For each ts: price of the first pool trade at or after ts (what a copier buying then would get, before
    slippage). One trades call serves every ts. If GeckoTerminal hasn't indexed trades up to a very recent ts
    yet, wait for it (up to ~45 s) rather than guess."""
    trades = []
    for attempt in range(4):
        try:
            d = http(f"{C.GECKO}/pools/{pool}/trades", kind="gecko")
            trades = []
            for t in (d or {}).get("data") or []:
                a = t["attributes"]
                tt = calendar.timegm(time.strptime(a["block_timestamp"], "%Y-%m-%dT%H:%M:%SZ"))
                if a["from_token_address"] == mint:
                    px = float(a["price_from_in_usd"])
                elif a["to_token_address"] == mint:
                    px = float(a["price_to_in_usd"])
                else:
                    continue
                trades.append((tt, px))
            trades.sort()
        except Exception as e:
            log.warning("gecko trades error %s", e)
        newest = trades[-1][0] if trades else 0
        if attempt < 3 and newest < max(tss) and now() - max(tss) < 60:
            time.sleep(15); continue
        break
    out = []; cs = None
    for ts in tss:
        r = None
        if trades and trades[0][0] <= ts:
            after = [t for t in trades if t[0] >= ts]
            if after and after[0][0] - ts <= 120:
                r = (after[0][1], "trade")
        if r is None:
            if cs is None:
                cs = gecko_candles(pool, mint, int(min(tss) // 60 * 60) - 60)
            for c in cs:
                if c[0] <= ts < c[0] + 60:
                    r = ((c[2] + c[3] + c[4]) / 3, "candle"); break
            if r is None:
                later = [c for c in cs if c[0] >= ts]
                if later and later[0][0] - ts <= 300:
                    r = (later[0][1], "candle_open")
        out.append(r or (None, None))
    return out


def gecko_price_at(pool, mint, ts):
    return gecko_prices_at(pool, mint, [ts])[0]


def rugcheck(mint):
    try:
        return http(C.RUGCHECK.format(mint), kind="rug", tries=2)
    except Exception as e:
        log.warning("rugcheck error %s %s", mint, e)
        return None


def analyse_rug(r, pool):
    """dev %, top-10 % (excluding pools/lockers/curve), insider %, graph insiders, authorities."""
    tok = r.get("token") or {}
    supply = float(tok.get("supply") or 0); dec = tok.get("decimals") or 0
    creator = r.get("creator")
    known = r.get("knownAccounts") or {}
    excl = set()
    for addr, info in known.items():
        if (info or {}).get("type") in ("AMM", "LOCKER") or "pool" in (info or {}).get("name", "").lower() \
                or "curve" in (info or {}).get("name", "").lower():
            excl.add(addr)
    for m in r.get("markets") or []:
        for k in ("pubkey", "liquidityA", "liquidityB", "liquidityAAccount", "liquidityBAccount"):
            v = m.get(k)
            if isinstance(v, str):
                excl.add(v)
            elif isinstance(v, dict) and v.get("owner"):
                excl.add(v["owner"])
    if pool:
        excl.add(pool)
    excl.add("1nc1nerator11111111111111111111111111111111")       # burn address: burned coins belong to nobody
    holders = [h for h in (r.get("topHolders") or []) if h.get("owner") not in excl and h.get("address") not in excl]
    top10 = sum(float(h.get("pct") or 0) for h in holders[:10])
    top1 = max([float(h.get("pct") or 0) for h in holders] or [0.0])     # biggest single real holder, logged only
    insider = sum(float(h.get("pct") or 0) for h in holders if h.get("insider"))
    dev = 0.0
    if supply > 0 and r.get("creatorBalance"):
        dev = float(r["creatorBalance"]) / supply * 100
    for h in r.get("topHolders") or []:
        if creator and h.get("owner") == creator:
            dev = max(dev, float(h.get("pct") or 0))
    risks = [x.get("name", "") for x in r.get("risks") or []]
    liq = r.get("totalMarketLiquidity")
    tf = (r.get("transferFee") or {}).get("pct")
    lp = None
    for m in r.get("markets") or []:
        if m.get("pubkey") == pool:
            lp = (m.get("lp") or {}).get("lpLockedPct")
    return dict(holders=r.get("totalHolders"), transfer_fee=float(tf) if tf is not None else None, lp_locked=lp,
                token_program=r.get("tokenProgram") or r.get("tokenType"),
                dev=dev, top10=top10, top1=top1, insider=insider, graph=int(r.get("graphInsidersDetected") or 0),
                mint_auth=1 if tok.get("mintAuthority") else 0, freeze_auth=1 if tok.get("freezeAuthority") else 0,
                score=r.get("score_normalised"), risks=risks, rugged=bool(r.get("rugged")), liq=liq)


# ----------------------------------------------------------------------------- costs
def buy_qty(size_usd, px):
    return size_usd * (1 - C.BOT_FEE - C.POOL_FEE) / (px * (1 + C.SLIPPAGE))


def sell_proceeds(qty, px):
    return max(0.0, qty * px * (1 - C.SLIPPAGE) * (1 - C.BOT_FEE - C.POOL_FEE) - C.NETWORK_FEE_USD)


# ----------------------------------------------------------------------------- Filter + Sniper
def process_new_buys(con, cands):
    """cands: list of dicts(wallet, name, mint, ata, amt, prev_ts, det_ts[, src, tx])."""
    if not cands:
        return
    pairs = dex_pairs({c["mint"] for c in cands})
    for c in cands:
        try:
            if handle_candidate(con, c, pairs.get(c["mint"]) or []) == "retry":
                c["retry_at"] = now() + 20
                PENDING.append(c)
        except Exception as e:
            log.error("candidate error %s %s\n%s", c["mint"], e, traceback.format_exc())
        con.commit()


def insert_signal(con, **kw):
    cols = ",".join(kw.keys()); q = ",".join("?" * len(kw))
    cur = con.execute(f"INSERT INTO signals({cols}) VALUES({q})", tuple(kw.values()))
    return cur.lastrowid


def handle_candidate(con, c, pairs):
    mint, wallet, det = c["mint"], c["wallet"], c["det_ts"]
    src_det = c.get("src", "poll"); txi = c.get("tx")
    tp = top_pair(pairs)
    base = dict(detected_ts=det, wallet=wallet, wallet_name=c["name"], mint=mint, ata=c["ata"], detect_src=src_det)
    if mint in C.EXCLUDE_MINTS:
        return
    if not tp:
        # a coin bought seconds ago may not be on DexScreener yet: retry for a couple of minutes first
        if txi and txi["paid"] and now() - det < C.DEX_RETRY_S:
            return "retry"
        insert_signal(con, **base, status="ignored", notes="no DEX market (airdrop/spam/unlisted)"); return
    price = float(tp.get("priceUsd") or 0)
    sym = tp["baseToken"].get("symbol"); nm = tp["baseToken"].get("name")
    dec_guess = None
    # leader buy tx: exact time, and confirm it was a swap (not an airdrop/transfer)
    buy_ts = None; buy_sig = None; post_amt = None; note = []
    if txi:   # live feed: the buy transaction itself is already in hand
        if not txi["paid"]:
            insert_signal(con, **base, symbol=sym, name=nm, status="ignored", notes="received by transfer, not bought")
            return
        buy_ts, buy_sig, post_amt, dec_guess = txi["block_ts"], txi["sig"], txi["post"], txi["dec"]
    else:
        try:
            hist = ata_history(c["ata"], c["prev_ts"])
            for s in hist[:4]:
                tx = get_tx(s["signature"])
                if not tx:
                    continue
                pre, post, dec = token_delta(tx, wallet, mint)
                if post > pre:
                    if not paid_something(tx, wallet):
                        insert_signal(con, **base, symbol=sym, name=nm, status="ignored", notes="received by transfer, not bought")
                        return
                    buy_ts, buy_sig, post_amt, dec_guess = s["blockTime"], s["signature"], post, dec
                    break
        except Exception as e:
            note.append(f"buy tx lookup failed: {e}")
    if buy_ts is None:
        buy_ts = (c["prev_ts"] + det) / 2; note.append("buy time estimated")
    dec = dec_guess if dec_guess is not None else 6
    leader_usd = c["amt"] / (10 ** dec) * price if dec_guess is not None else None
    if leader_usd is not None and leader_usd < C.MIN_LEADER_BUY_USD:
        insert_signal(con, **base, symbol=sym, name=nm, status="ignored", notes=f"leader bag only ${leader_usd:.2f}"); return
    lag = det - buy_ts
    liq_dex = sum(((p.get("liquidity") or {}).get("usd") or 0) for p in pairs)
    h1 = (tp.get("priceChange") or {}).get("h1")
    age_min = (det - tp["pairCreatedAt"] / 1000) / 60 if tp.get("pairCreatedAt") else None
    sig = dict(base, symbol=sym, name=nm, leader_buy_ts=buy_ts, leader_buy_sig=buy_sig, leader_post_amt=post_amt,
               leader_usd=leader_usd, detect_lag_s=lag, pool=tp["pairAddress"], dex=tp.get("dexId"), price_detect=price,
               liq_usd=liq_dex or None, h1_pct=h1, mcap=tp.get("marketCap") or tp.get("fdv"), pair_age_min=age_min)
    # repeat buys of a coin already signalled in the last 24h are confluence, not a new signal
    prev = con.execute("SELECT id FROM signals WHERE mint=? AND status NOT IN ('ignored','repeat') AND detected_ts>?",
                       (mint, det - 86400)).fetchone()
    if prev:
        insert_signal(con, **sig, status="repeat", notes=f"same coin as signal #{prev[0]}"); return
    if lag > C.MISSED_LAG_S:
        insert_signal(con, **sig, status="missed", notes="; ".join(note + [f"found {lag/60:.0f} min late"])); return

    # Two entry times. Realistic (headline): the moment the scanner saw the buy. Old test: 5 s after the wallet.
    entry_real = det
    entry_5s = buy_ts + C.COPY_DELAY_S
    # --- cheap filters
    # smart wallet sold before we could copy (only sells before our entry time count: no look-ahead)
    sold_real = sold_5s = False
    try:
        hist2 = ata_history(c["ata"], buy_ts)
        horizon = max(entry_real, entry_5s)
        for s in hist2:
            if s["blockTime"] > horizon:
                break
            if s["signature"] == buy_sig:
                continue
            tx = get_tx(s["signature"])
            if not tx:
                continue
            pre, post, _ = token_delta(tx, wallet, mint)
            if post < pre:
                sold_real = s["blockTime"] <= entry_real
                sold_5s = s["blockTime"] <= entry_5s
                break
    except Exception as e:
        note.append(f"sold-early check failed: {e}")
    other = []
    if h1 is not None and h1 > C.PUMP_1H_MAX_PCT:
        other.append("up_100pct_1h")
    killed = (["smart_wallet_sold_early"] if sold_real else []) + other
    killed5 = (["smart_wallet_sold_early"] if sold_5s else []) + other
    # --- RugCheck (dev, top10, bundle, authorities). Skipped when a cheap rule already killed the coin
    # (for both entry versions) and DexScreener liquidity is known, to stay inside RugCheck's free limit.
    if killed and killed5 and liq_dex:
        if liq_dex < C.LIQ_MIN_USD:
            killed.append("liquidity_under_25k")
        sig["killed_by"] = ",".join(killed)
        insert_signal(con, **sig, status="killed", notes="; ".join(note + ["holder checks not run"])); return
    r = rugcheck(mint)
    if r is None:
        time.sleep(5); r = rugcheck(mint)
    a = analyse_rug(r, tp["pairAddress"]) if r else None
    liq = liq_dex if liq_dex else (a["liq"] if a else None)
    sig["liq_usd"] = liq
    rest = []
    if liq is None or liq < C.LIQ_MIN_USD:
        rest.append("liquidity_under_25k")
    if a:
        sig.update(dev_pct=a["dev"], top10_pct=a["top10"], insider_pct=a["insider"], graph_insiders=a["graph"],
                   mint_auth=a["mint_auth"], freeze_auth=a["freeze_auth"], rug_score=a["score"],
                   holders=a["holders"], transfer_fee_pct=a["transfer_fee"], lp_locked_pct=a["lp_locked"],
                   token_program=a["token_program"])
        if a["dev"] > C.DEV_MAX_PCT:
            rest.append("dev_over_5pct")
        if a["top10"] > C.TOP10_MAX_PCT:
            rest.append("top10_over_35pct")
        elif a["top10"] > C.MAIN_TOP10_MAX_PCT and "top10_over_32.5pct" not in killed:
            killed.append("top10_over_32.5pct")          # main (realistic) accounts only; 5 s test keeps 35%
        note.append(f"rugcheck biggest real holder {a['top1']:.1f}%, top-10 {a['top10']:.1f}% (pools/curve/burn left out)")
        if a["insider"] >= C.BUNDLE_INSIDER_PCT or any(re.search(r"bundl|insider", x, re.I) for x in a["risks"]):
            rest.append("bundled_buys")
        if a["mint_auth"]:
            rest.append("analyst_mint_authority_live")
        if a["freeze_auth"]:
            rest.append("analyst_freeze_authority_live")
        if a["rugged"]:
            rest.append("analyst_flagged_rugged")
        if a["risks"]:
            note.append("rugcheck: " + ", ".join(a["risks"])[:200])
    else:
        rest.append("safety_check_unavailable")
    killed += rest; killed5 += rest
    sig["killed_by"] = ",".join(killed) or None
    if killed and killed5:
        insert_signal(con, **sig, status="killed", notes="; ".join(note) or None); return

    # --- Sniper: survivor -> paper entries (realistic headline, plus the old 5 s test)
    pool = tp["pairAddress"]
    sig.update(holder_check(mint, pool, wallet, tp.get("dexId") == "pumpfun"))
    rug_why = [f"top 10 holders own {sig['hf_top10']:.1f}% (limit {C.HF_TOP10_MAX:.0f}%)"] if sig.get("hf_block") else []
    hf_takes = not rug_why
    sid = None
    fill_ts = entry_real + C.EXEC_DELAY_S
    (px_seen, src_r), (px_5, src_5), (px_lead, _), (px_fill, src_f) = gecko_prices_at(
        pool, mint, [entry_real, entry_5s, buy_ts, fill_ts])
    if not px_seen:
        px_seen, src_r = price, "dexscreener_live"; note.append("no trade data at detection time, used DexScreener price")
    if not px_5:
        px_5, src_5 = price, "dexscreener_live"
    fill = None
    if not killed:
        # live-like fill for the realistic accounts: lands EXEC_DELAY_S later, priced with real quotes and fees
        px_r = px_fill or px_seen
        q = fills.entry_quote(mint, pool, 20.0, liq)
        fee_in = fills.tx_fee_usd(pool)
        fill = dict(fill_model="live", leader_px=px_lead, seen_px=px_seen, r_buy=q["r_buy"], r_sell_est=q["r_sell"],
                    rt_pct=q["rt_pct"], q_src_in=q["src"], route=q.get("route"), fee_in=fee_in, liq_in=liq,
                    sellable=1 if q["sellable"] else 0)
        sig["rt_pct"] = q["rt_pct"]
        if q["rt_pct"] is not None and q["rt_pct"] >= C.HF_RT_MAX:
            rug_why.append(f"round trip {q['rt_pct']:.1f}% (limit {C.HF_RT_MAX:.0f}%)")
        hf_takes = not rug_why
        sig["hf_block"] = 1 if rug_why else 0
        fn = [f"live-like fill: lands {C.EXEC_DELAY_S}s after seen at {px_r:.6g} (seen {px_seen:.6g})",
              f"round trip {q['rt_pct']:.1f}% via {q['src']}", f"network ${fee_in:.3f}/tx"]
        if not q["sellable"]:
            fn.append("Jupiter could buy but not sell it back (honeypot or sell tax)")
        jump = px_r / px_seen - 1 if px_seen else 0
        if jump > C.COPY_SLIPPAGE_TOL:
            # a live bot's buy fails on its slippage limit; the failed transaction's fee is still paid
            fail_fee = fills.tx_fee_usd(pool, landed=False)
            fn.append(f"buy failed: price jumped {jump*100:.0f}% before landing (limit {C.COPY_SLIPPAGE_TOL*100:.0f}%), "
                      f"${fail_fee:.3f} fee lost")
            meta_set(con, cash_key(C.ACCT_REAL), meta_get(con, cash_key(C.ACCT_REAL), C.START_EQUITY) - fail_fee)
            if hf_takes and hf_started(con):   # the rug-filter test would have tried the same buy
                meta_set(con, cash_key(C.ACCT_HF), meta_get(con, cash_key(C.ACCT_HF), C.START_EQUITY) - fail_fee)
            fill = "failed"
        sig["fill_note"] = "; ".join(fn)
    if killed:   # only the realistic sold-early check blocked it: the wallet sold after 5 s but before we saw the buy
        sid = insert_signal(con, **sig, status="killed",
                            notes="; ".join(note + ["5 s test still copied it (wallet sold before the scanner saw the buy)"]))
    else:
        if fill == "failed":
            sid = insert_signal(con, **sig, status="survivor",
                                notes="; ".join(note + ["realistic buy failed (price ran past the slippage limit), not copied"]))
        else:
            sid = insert_signal(con, **sig, status="survivor", notes="; ".join(note) or None)
            # shadow position: every survivor, flat $20, no slot limit (measures the filters themselves)
            open_position(con, sid, C.ACCT_REAL_SHADOW, mint, sym, pool, wallet, fill_ts, px_r, src_f or src_r, 20.0, fill)
            open_main(con, sid, C.ACCT_REAL, mint, sym, pool, wallet, fill_ts, px_r, src_f or src_r, c["name"], "", fill)
            # shadow test: the same trade with the rug filter (the current rules above are unchanged)
            if not hf_started(con):
                pass
            elif hf_takes:
                open_main(con, sid, C.ACCT_HF, mint, sym, pool, wallet, fill_ts, px_r, src_f or src_r, c["name"],
                          "rug-filter test: ", fill)
            else:
                con.execute("UPDATE signals SET notes=COALESCE(notes||'; ','')||? WHERE id=?",
                            ("rug-filter test: skipped, " + ", ".join(rug_why), sid))
                log.info("RUG FILTER would skip %s %s: %s", sym, mint, ", ".join(rug_why))
    if sid:
        queue_chain_info(sid, mint, pool, wallet, det, tp.get("dexId") == "pumpfun")
    if not killed5:
        open_position(con, sid, C.ACCT_5S_SHADOW, mint, sym, pool, wallet, entry_5s, px_5, src_5, 20.0)
        if open_main(con, sid, C.ACCT_5S, mint, sym, pool, wallet, entry_5s, px_5, src_5, c["name"], "5 s test: "):
            live_hook(live.on_paper_buy, con, sid, mint, sym, buy_ts)


def holder_check(mint, pool, wallet, is_curve):
    """Top-10 holder numbers for the holder-filter shadow test. Never blocks the current rules."""
    try:
        h = holders.top10(mint, pool if is_curve else None, wallet)
    except Exception as e:
        log.warning("holder check failed for %s: %s", mint, e)
        return dict(hf_block=0, hf_info=json.dumps({"error": str(e)[:200]}))
    block = 1 if (is_curve and h["top10"] >= C.HF_TOP10_MAX) else 0
    info = {k: h[k] for k in ("top10", "top1", "curve_pct", "leader_pct", "counted", "secs")}
    info["applies"] = bool(is_curve)
    info["top_owners"] = h["top_owners"]
    return dict(hf_top10=h["top10"], hf_top1=h["top1"], hf_block=block, hf_info=json.dumps(info))


def hf_started(con):
    return meta_get(con, "hf_start_ts") is not None


CHAIN_POOL = None
CHAIN_JOBS = []


def queue_chain_info(sid, mint, pool, wallet, det, is_curve):
    """Creator and early-buyer numbers, read in the background so they never delay a paper buy."""
    global CHAIN_POOL
    try:
        if CHAIN_POOL is None:
            CHAIN_POOL = ThreadPoolExecutor(max_workers=1)
        # chain_info + the stronger holder checks (logged only, never block or sell; Nov 7 test)
        CHAIN_JOBS.append((sid, CHAIN_POOL.submit(holders.chain_and_holder_info, mint, pool, wallet, det, is_curve, pool)))
    except Exception as e:
        log.warning("chain info not queued for %s: %s", mint, e)


def drain_chain_info(con):
    for job in list(CHAIN_JOBS):
        sid, fut = job
        if not fut.done():
            continue
        CHAIN_JOBS.remove(job)
        try:
            ci = fut.result()
        except Exception as e:
            ci = {"error": str(e)[:200]}
        con.execute("UPDATE signals SET chain_info=? WHERE id=?", (json.dumps(ci), sid))
        con.commit()


def cash_key(account):
    return {C.ACCT_REAL: "cash_real", C.ACCT_HF: "cash_hf"}.get(account, "cash")


def open_main(con, sid, account, mint, sym, pool, wallet, entry_ts, px, src, name, tag, fill=None):
    """A $200 paper account: 10% of equity per trade, max 5 open."""
    st = account_state(con, account)
    size = max(C.MIN_POS_USD, st["equity"] * C.POS_FRACTION)
    fee = fill["fee_in"] if fill else C.NETWORK_FEE_USD
    if st["n_open"] >= C.MAX_OPEN:
        con.execute("UPDATE signals SET notes=COALESCE(notes||'; ','')||? WHERE id=?", (f"{tag}account full (5 open), not copied", sid))
        return False
    if st["cash"] < size + fee:
        con.execute("UPDATE signals SET notes=COALESCE(notes||'; ','')||? WHERE id=?", (f"{tag}not enough paper cash", sid))
        return False
    open_position(con, sid, account, mint, sym, pool, wallet, entry_ts, px, src, size, fill)
    meta_set(con, cash_key(account), st["cash"] - size - fee)
    log.info("PAPER BUY [acct %d] %s %s $%.2f @ %.10g (%s) copying %s", account, sym, mint, size, px, src, name)
    return True


def open_position(con, sid, account, mint, sym, pool, leader, entry_ts, px, src, size, fill=None):
    stop = px * (1 - C.MAX_LOSS_PCT)
    qty = fills.buy_qty(size, px, fill["r_buy"]) if fill else buy_qty(size, px)
    if fill:
        fee = fill["fee_in"]; sf = (fill["r_sell_est"] or 0) * (1 - C.BOT_FEE)
    else:
        fee = C.NETWORK_FEE_USD; sf = (1 - C.SLIPPAGE) * (1 - C.BOT_FEE - C.POOL_FEE)
    be_px = (size + 2 * fee) / (qty * sf) if qty and sf > 0 else px * C.BE_LEVEL
    alts = {k: alt_init(spec, px, qty, entry_ts, be_px) for k, spec in ALTS.items()}
    fill = dict(fill or {}, rules="v3", be_px=be_px, alts=json.dumps(alts), alts_open=len(alts))
    cur = con.execute("""INSERT INTO positions(signal_id,account,mint,symbol,pool,leader,entry_ts,entry_price,entry_src,size_usd,qty,
        stop,peak,last_candle_ts,last_price,last_update,status,stop_log) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'open', ?)""",
                (sid, account, mint, sym, pool, leader, entry_ts, px, src, size, qty, stop, px,
                 int(entry_ts // 60 * 60), px, 0, json.dumps([[entry_ts, stop, "$5 max-loss (-25%)"]])))
    if fill:
        cols = list(fill.keys())
        con.execute(f"UPDATE positions SET {','.join(c + '=?' for c in cols)} WHERE id=?",
                    tuple(fill[c] for c in cols) + (cur.lastrowid,))


def paper_reset(con):
    """One-time paper balance reset (config PAPER_RESET), applied once per reset id. Paper only."""
    R = getattr(C, "PAPER_RESET", None)
    if not R:
        return
    done = meta_get(con, "paper_resets_done", []) or []
    if R["id"] in done:
        return
    t = now()
    curve = {C.ACCT_5S: "equity", C.ACCT_REAL: "equity_real", C.ACCT_HF: "equity_hf"}
    start = {C.ACCT_5S: "start_ts", C.ACCT_REAL: "real_start_ts", C.ACCT_HF: "hf_start_ts"}
    sessions = meta_get(con, "session_ts", {}) or {}
    for acct in R["accounts"]:
        st = account_state(con, acct)
        since = sessions.get(str(acct)) or meta_get(con, start[acct]) or 0
        n, w, pnl = con.execute("SELECT COUNT(*), COALESCE(SUM(pnl_usd>0),0), COALESCE(SUM(pnl_usd),0) FROM positions "
                                "WHERE account=? AND status='closed' AND COALESCE(exit_ts,0)>=?", (acct, since)).fetchone()
        cash_after = C.START_EQUITY - st["open_value"]
        con.execute("INSERT OR REPLACE INTO paper_resets VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (R["id"], t, acct, since, st["cash"], st["open_value"], st["equity"], st["n_open"], n, w, pnl, cash_after))
        meta_set(con, cash_key(acct), cash_after)
        con.execute(f"INSERT OR REPLACE INTO {curve[acct]} VALUES(?,?,?,?,?)",
                    (t, cash_after, st["open_value"], C.START_EQUITY, st["n_open"]))
        sessions[str(acct)] = t
        log.info("PAPER RESET [acct %d] equity $%.2f -> $%.2f (cash $%.2f -> $%.2f, %d open worth $%.2f kept); "
                 "old session archived: %d closed, %d won, $%+.2f", acct, st["equity"], C.START_EQUITY, st["cash"],
                 cash_after, st["n_open"], st["open_value"], n, w, pnl)
    meta_set(con, "session_ts", sessions)
    meta_set(con, "paper_resets_done", done + [R["id"]])
    con.commit()


def manual_close(con):
    """One-time manual close (config MANUAL_CLOSE), applied once per id at scanner start. Paper only."""
    M = getattr(C, "MANUAL_CLOSE", None)
    if not M:
        return
    done = meta_get(con, "manual_closes_done", []) or []
    if M["id"] in done:
        return
    t = now()
    logbook = meta_get(con, "manual_close_log", []) or []
    if t > M.get("expires_ts", float("inf")):
        log.warning("MANUAL CLOSE %s expired before it could finish; nothing more closed", M["id"])
        meta_set(con, "manual_closes_done", done + [M["id"]]); con.commit()
        return
    mints = list(M["mints"])
    rows = con.execute(f"SELECT * FROM positions WHERE status='open' AND entry_ts<? AND mint IN ({','.join('?' * len(mints))}) "
                       "ORDER BY account", [M["requested_ts"]] + mints).fetchall()
    quotes = live_quotes([r["mint"] for r in rows]) if rows else {}
    pending = 0
    for r in rows:
        p = dict(r)
        e = quotes.get(p["mint"])
        if not e:
            pending += 1
            log.warning("MANUAL CLOSE [acct %d] %s: no live price, not closed (retried next start)", p["account"], p["symbol"])
            continue
        qt, px, src = e
        part = p.get("part_proceeds") or 0
        fee_in = (p.get("fee_in") or 0) if is_live(p) else C.NETWORK_FEE_USD
        est = fills.position_value(p, px) + part - p["size_usd"] - fee_in
        if M.get("only_if_loss") and est >= 0:
            log.info("MANUAL CLOSE [acct %d] %s left open: in profit at the live %s price %.10g (est $%+.2f)",
                     p["account"], p["symbol"], src, px, est)
            logbook.append([t, M["id"], p["id"], p["account"], p["symbol"], px, src, "left open (in profit)", est])
            continue
        slog = json.loads(p["stop_log"] or "[]")
        slog.append([qt, p["stop"], f"closed manually by Rage at the live {src} price"])
        close_position(con, p, qt, px, f"closed manually by Rage (live {src} price)", stop=p["stop"], peak=p["peak"], slog=slog)
        con.execute("UPDATE positions SET price_src=?, price_ts=? WHERE id=?", (src, qt, p["id"]))
        got = con.execute("SELECT pnl_usd FROM positions WHERE id=?", (p["id"],)).fetchone()[0]
        log.info("MANUAL CLOSE [acct %d] %s closed at the live %s price %.10g, pnl $%+.2f", p["account"], p["symbol"], src, px, got)
        logbook.append([t, M["id"], p["id"], p["account"], p["symbol"], px, src, "closed", got])
    meta_set(con, "manual_close_log", logbook[-50:])
    if not pending:
        meta_set(con, "manual_closes_done", done + [M["id"]])
        log.info("MANUAL CLOSE %s done (%d open trades checked)", M["id"], len(rows))
    con.commit()


def account_state(con, account=C.ACCT_5S):
    cash = meta_get(con, cash_key(account), C.START_EQUITY)
    opens = con.execute("SELECT * FROM positions WHERE account=? AND status='open'", (account,)).fetchall()
    ov = sum(fills.position_value(o, o["last_price"] or 0) for o in opens)
    return dict(cash=cash, open_value=ov, equity=cash + ov, n_open=len(opens))


# ----------------------------------------------------------------------------- Guard
def new_stop(entry, peak, old):
    gain = peak / entry - 1 + 1e-9
    s = entry * (1 - C.INITIAL_STOP)
    for trig, dist in C.TRAIL_STEPS:
        if gain >= trig:
            s = peak * (1 - dist)
    if gain >= C.BE_TRIGGER:
        s = max(s, entry * C.BE_LEVEL)
    return max(old, s)


REAL_SPEC = dict(max_loss=C.MAX_LOSS_PCT, be=C.BE2_TRIGGER, half_at=C.HALF_AT, trail=C.TRAIL_V3)
ALTS = {
    # the old exits on the same trade: $5 stop + breakeven, the old tightening trail, no half sell
    "old_exits": dict(max_loss=C.MAX_LOSS_PCT, be=C.BE2_TRIGGER, half_at=None, trail=C.TRAIL_STEPS),
    # the new exits without the $5 stop and the breakeven rule
    "no_stop_be": dict(max_loss=None, be=None, half_at=C.HALF_AT, trail=C.TRAIL_V3),
}


def stop_for(spec, entry, peak, old, be_px):
    gain = peak / entry - 1 + 1e-9
    s = entry * (1 - spec["max_loss"]) if spec.get("max_loss") else 0.0
    for trig, dist in spec["trail"]:
        if gain >= trig:
            s = peak * (1 - dist)
    if spec.get("be") and gain >= spec["be"]:
        s = max(s, be_px or entry * C.BE_LEVEL)
    return max(old, s)


def alt_init(spec, px, qty, entry_ts, be_px):
    return {"st": "open", "stop": stop_for(spec, px, px, 0.0, be_px), "peak": px, "last": int(entry_ts // 60 * 60),
            "px": px, "frac": 1.0, "q0": qty, "legs": []}


def is_v3(p):
    return "rules" in p.keys() and p["rules"] == "v3"


def stop_reason(p, stop, gapped):
    entry = p["entry_price"]
    if is_v3(p):
        if stop < entry:
            r = "max-loss stop"
        elif p["be_px"] and abs(stop - p["be_px"]) <= 1e-9 * stop:
            r = "breakeven stop"
        else:
            r = "trailing stop"
        return r + (" (gapped)" if gapped else "")
    if stop < entry:
        return "stop (gapped through)" if gapped else "stop"
    return "trailing stop (gapped)" if gapped else "trailing stop"


TRADES = {}


def stop_fill(p, t, stop):
    """Realistic sell after a stop triggers: find the first real pool trade at or under the stop in this minute,
    then sell at the first trade EXEC_DELAY_S after it. Dumps and rugs fill worse than the stop, as live."""
    key = p["pool"]
    e = TRADES.get(key)
    if not (e and now() - e[0] < 30):
        try:
            d = http(f"{C.GECKO}/pools/{p['pool']}/trades", kind="gecko")
            tr = []
            for x in (d or {}).get("data") or []:
                a = x["attributes"]
                tt = calendar.timegm(time.strptime(a["block_timestamp"], "%Y-%m-%dT%H:%M:%SZ"))
                if a["from_token_address"] == p["mint"]:
                    tr.append((tt, float(a["price_from_in_usd"])))
                elif a["to_token_address"] == p["mint"]:
                    tr.append((tt, float(a["price_to_in_usd"])))
            tr.sort(); TRADES[key] = (now(), tr); e = TRADES[key]
        except Exception as ex:
            log.warning("stop fill trades error %s %s", p["symbol"], ex)
            return None
    tr = e[1]
    if not tr or tr[0][0] >= t + 60:
        return None
    hit = next((x for x in tr if t <= x[0] < t + 60 and x[1] <= stop), None)
    if not hit:
        return None
    after = [x for x in tr if x[0] >= hit[0] + C.EXEC_DELAY_S]
    if after and after[0][0] - hit[0] <= 120:
        return after[0][1]
    return hit[1]


def is_live(p):
    return "fill_model" in p.keys() and p["fill_model"] == "live"


def sell_leg(p, qty, px, ts, t_now):
    """Proceeds of selling qty at px. Live-like: re-quote now if the sell is recent, else the entry-time estimate."""
    if not is_live(p):
        return sell_proceeds(qty, px), None, C.NETWORK_FEE_USD
    r = p["r_sell_est"] or 0
    if t_now - ts < 600:
        try:
            pr = dex_pairs([p["mint"]]).get(p["mint"]) or []
            liq = sum(((x.get("liquidity") or {}).get("usd") or 0) for x in pr) or None
            r = fills.exit_quote(p["mint"], qty, liq if liq is not None else (p["liq_in"] or 0), bool(p["sellable"]))["r_sell"]
        except Exception as ex:
            log.warning("leg quote error %s %s", p["symbol"], ex)
    fee = fills.tx_fee_usd(p["pool"]) if r > 0 else 0.0
    return fills.sell_value(qty, px, r, fee), r, fee


def stop_px(p, t, stop, o, c, live_fill):
    if o <= stop:
        px, src = o, "candle"
    else:
        px, src = (stop if c >= stop else c), "candle"
    if live_fill:
        f = stop_fill(p, t, stop)
        if f:
            px, src = f, "trade after trigger"
    return px, src


def alt_step(con, p, cs, mirror_ts, t_now, live_fill):
    """Advance the comparison copies of this trade (other exit rules) over the same candles."""
    if not ("alts" in p.keys() and p["alts"]):
        return
    alts = json.loads(p["alts"]); entry = p["entry_price"]; be = p["be_px"]
    entry_fee = (p["fee_in"] or 0) if is_live(p) else C.NETWORK_FEE_USD
    for name, a in alts.items():
        if a["st"] != "open" or name not in ALTS:
            continue
        spec = ALTS[name]

        def leg(ts, px, frac):
            v, r, fee = sell_leg(p, a["q0"] * frac, px, ts, t_now)
            a["legs"].append([ts, px, frac, v])

        def done(ts, px, why):
            leg(ts, px, a["frac"]); a["frac"] = 0.0
            a.update(st="closed", exit_ts=ts, exit_px=px, reason=why,
                     pnl=sum(x[3] for x in a["legs"]) - p["size_usd"] - entry_fee)

        for (t, o, h, l, c) in cs:
            if t <= p["entry_ts"] - 60 or t <= a["last"]:
                continue
            if mirror_ts and t >= mirror_ts:
                done(t, o, "smart wallet sold"); break
            if t >= p["entry_ts"] + C.TIME_STOP_S:
                done(t, o, "24h time stop"); break
            if a["stop"] and (o <= a["stop"] or l <= a["stop"]):
                px, _ = stop_px(p, t, a["stop"], o, c, live_fill)
                done(t, px, stop_reason(p, a["stop"], o <= a["stop"])); break
            if spec.get("half_at") and a["frac"] == 1.0 and h >= entry * spec["half_at"]:
                leg(t, max(o, entry * spec["half_at"]), C.HALF_FRAC); a["frac"] = 1.0 - C.HALF_FRAC
            if h > a["peak"]:
                a["peak"] = h; a["stop"] = stop_for(spec, entry, a["peak"], a["stop"], be)
            a["last"], a["px"] = t, c
        if a["st"] == "open" and t_now >= p["entry_ts"] + C.TIME_STOP_S and a["last"] < p["entry_ts"] + C.TIME_STOP_S - 60:
            if mirror_ts and mirror_ts < p["entry_ts"] + C.TIME_STOP_S:
                done(mirror_ts, a["px"], "smart wallet sold")
            else:
                done(p["entry_ts"] + C.TIME_STOP_S, a["px"], "24h time stop")
        a["upd"] = t_now
    n_open = sum(1 for a in alts.values() if a["st"] == "open")
    con.execute("UPDATE positions SET alts=?, alts_open=? WHERE id=?", (json.dumps(alts), n_open, p["id"]))


def mirror_time(con, p):
    ls = con.execute("SELECT ts,sold_frac,det_ts FROM leader_sells WHERE wallet=? AND mint=?", (p["leader"], p["mint"])).fetchone()
    if ls and ls["sold_frac"] >= C.MIRROR_SELL_FRAC:
        if p["account"] in (C.ACCT_REAL, C.ACCT_REAL_SHADOW, C.ACCT_HF) and ls["det_ts"]:
            return max(ls["det_ts"], ls["ts"])
        return ls["ts"] + C.COPY_DELAY_S
    return None


def alt_all(con):
    """Keep following trades the real rules already closed but a comparison copy still holds."""
    rows = con.execute("""SELECT * FROM positions WHERE status='closed' AND alts_open>0
                          AND COALESCE(last_update,0) < ? ORDER BY last_update LIMIT 6""", (now() - 120,)).fetchall()
    for p in rows:
        if LIM["gecko"].free() < 4:
            break
        try:
            t_now = now()
            alts = json.loads(p["alts"])
            since = min(a["last"] for a in alts.values() if a["st"] == "open") + 60
            cs = candles_cached(p["pool"], p["mint"], int(since))
            cs = [c for c in cs if c[0] + 60 <= t_now - 20]
            alt_step(con, p, cs, mirror_time(con, p), t_now, p["account"] in (C.ACCT_REAL, C.ACCT_REAL_SHADOW, C.ACCT_HF))
            con.execute("UPDATE positions SET last_update=? WHERE id=?", (t_now, p["id"]))
            con.commit()
        except Exception as ex:
            log.warning("alt exits error %s %s", p["symbol"], ex)


def take_half(con, p, t, px, stop, slog, t_now):
    """Runner plan: at 2x sell half the bag, so the stake comes back and the rest rides under the trail."""
    half = p["qty"] * C.HALF_FRAC
    proceeds, r, fee = sell_leg(p, half, px, t, t_now)
    p["qty"] = p["qty"] - half
    p["part_proceeds"] = proceeds; p["part_r"] = r; p["part_fee"] = fee; p["part_px"] = px
    con.execute("""UPDATE positions SET qty=?, part_ts=?, part_px=?, part_qty=?, part_proceeds=?, part_r=?, part_fee=? WHERE id=?""",
                (p["qty"], t, px, half, proceeds, r, fee, p["id"]))
    slog.append([t, stop, f"sold half at {px / p['entry_price']:.1f}x for ${proceeds:.2f}"])
    if p["account"] in (C.ACCT_5S, C.ACCT_REAL, C.ACCT_HF):
        k = cash_key(p["account"])
        meta_set(con, k, meta_get(con, k, C.START_EQUITY) + proceeds)
        log.info("PAPER SELL HALF [acct %d] %s at %.1fx $%.2f", p["account"], p["symbol"], px / p["entry_price"], proceeds)


def close_position(con, p, ts, px, reason, stop=None, peak=None, slog=None):
    if "fill_model" in p.keys() and p["fill_model"] == "live":
        proceeds, pnl = close_live(con, p, px)
    else:
        proceeds = sell_proceeds(p["qty"], px)
        pnl = proceeds + ((p["part_proceeds"] if "part_proceeds" in p.keys() else 0) or 0) - p["size_usd"] - C.NETWORK_FEE_USD
    con.execute("""UPDATE positions SET status='closed',exit_ts=?,exit_price=?,exit_reason=?,proceeds=?,pnl_usd=?,pnl_pct=?,last_price=?,
                   stop=COALESCE(?,stop),peak=COALESCE(?,peak),stop_log=COALESCE(?,stop_log),last_update=? WHERE id=?""",
                (ts, px, reason, proceeds, pnl, pnl / p["size_usd"] * 100, px, stop, peak,
                 json.dumps(slog[-30:]) if slog is not None else None, now(), p["id"]))
    if p["account"] in (C.ACCT_5S, C.ACCT_REAL, C.ACCT_HF):
        k = cash_key(p["account"])
        meta_set(con, k, meta_get(con, k, C.START_EQUITY) + proceeds)
        log.info("PAPER SELL [acct %d] %s %s pnl $%.2f (%.0f%%)", p["account"], p["symbol"], reason, pnl, pnl / p["size_usd"] * 100)
    if p["account"] == C.ACCT_5S:
        live_hook(live.on_paper_close, con, p["signal_id"], p["mint"], reason)


def close_live(con, p, px):
    """Exit of a live-like position: re-quote the sell for exactly this token amount now, pay this moment's network
    fee, and split the gap from the 'ideal' trade (wallet's own entry, no costs) into its causes."""
    try:
        pr = dex_pairs([p["mint"]]).get(p["mint"]) or []
        # pump.fun bonding-curve pairs carry no liquidity figure: 0 there means unknown, not empty
        liq_out = sum(((x.get("liquidity") or {}).get("usd") or 0) for x in pr) or None
    except Exception:
        liq_out = None
    q = fills.exit_quote(p["mint"], p["qty"], liq_out if liq_out is not None else (p["liq_in"] or 0), bool(p["sellable"]))
    r_sell = q["r_sell"]
    fee_out = fills.tx_fee_usd(p["pool"]) if r_sell > 0 else 0.0
    size, mid, rb = p["size_usd"], p["entry_price"], p["r_buy"] or 1.0
    proceeds = fills.sell_value(p["qty"], px, r_sell, fee_out)
    part = (p["part_proceeds"] if "part_proceeds" in p.keys() else 0) or 0
    pnl = proceeds + part - size - (p["fee_in"] or 0)
    lead = p["leader_px"] or mid
    # each sell leg as a share of the bag: the half sold at 2x (if any) and the rest sold now
    legs = [(px, r_sell, 1.0)]
    if part:
        legs = [(p["part_px"], p["part_r"] if p["part_r"] is not None else r_sell, C.HALF_FRAC), (px, r_sell, 1 - C.HALF_FRAC)]
    ideal = sum(f * size * (x / lead - 1) for x, r, f in legs)
    a = sum(f * size * (x / mid - 1) for x, r, f in legs)                          # after the copy delay, no costs
    b = sum(f * (size * (x * r) / (mid * rb) - size) for x, r, f in legs)          # after price impact and DEX fees
    c = sum(f * (size * (1 - C.BOT_FEE) ** 2 * (x * r) / (mid * rb) - size) for x, r, f in legs)   # after the bot fee
    netfee = (p["fee_in"] or 0) + fee_out + (((p["part_fee"] if "part_fee" in p.keys() else 0) or 0) if part else 0)
    con.execute("""UPDATE positions SET r_sell=?, q_src_out=?, fee_out=?, liq_out=?, ideal_pnl=?, gap_delay=?,
                   gap_impact=?, gap_botfee=?, gap_netfee=? WHERE id=?""",
                (r_sell, q["src"], fee_out, liq_out, ideal, ideal - a, a - b, b - c, netfee, p["id"]))
    return proceeds, pnl


QUOTES = {}   # mint -> (fetched_ts, usd_price, source), shared by every account holding the coin


def live_quotes(mints):
    """Current USD market price per mint, for coins whose candles have gone stale. Jupiter first (one batched call),
    DexScreener's biggest pool as backup. Cached 20 s. A coin with no answer is simply left out (never made up)."""
    t = now(); out = {}; need = []
    for m in set(mints):
        e = QUOTES.get(m)
        if e and t - e[0] < 20:
            out[m] = e
        else:
            need.append(m)
    for i in range(0, len(need), 50):
        chunk = need[i:i + 50]
        try:
            d = fills._jup("/price/v3?ids=" + ",".join(chunk)) or {}
            for m in chunk:
                v = (d.get(m) or {}).get("usdPrice")
                if v and float(v) > 0:
                    QUOTES[m] = out[m] = (now(), float(v), "jupiter")
        except Exception as ex:
            log.warning("jupiter price error %s", ex)
    rest = [m for m in need if m not in out]
    if rest:
        for m, ps in dex_pairs(rest).items():
            tp = top_pair(ps)
            try:
                v = float(tp["priceUsd"]) if tp and tp.get("priceUsd") else 0
            except (TypeError, ValueError):
                v = 0
            if v > 0:
                QUOTES[m] = out[m] = (now(), v, "dexscreener")
    return out


def candles_stale(last_candle_ts, entry_ts, t_now):
    return t_now - ((last_candle_ts or entry_ts) + 60) > C.QUOTE_STALE_S


def candle_since(p):
    return int(p["last_candle_ts"]) + 60 if p["last_update"] else int(p["entry_ts"] // 60 * 60) + 60


def candles_cached(pool, mint, since):
    """GeckoTerminal 1-minute candles, shared for 50 s by every position on the same coin (4 paper accounts)."""
    k = (pool, mint); e = CANDLES.get(k)
    if e and now() - e[0] < 50 and e[1] <= since:
        return [c for c in e[2] if c[0] >= since]
    cs = gecko_candles(pool, mint, since)
    CANDLES[k] = (now(), since, cs)
    return cs


def update_position(con, p, pool_now=None):
    p = dict(p)
    pool = pool_now or p["pool"]
    t_now = now()
    try:
        cs = candles_cached(pool, p["mint"], candle_since(p))
    except Exception as ex:   # no candles this time: the stale-candle guard below still checks the live price
        log.warning("candles error %s %s", p["symbol"], ex); cs = []
    cs = [c for c in cs if c[0] + 60 <= t_now - 20]   # completed candles only
    stop, peak, entry = p["stop"], p["peak"], p["entry_price"]
    slog = json.loads(p["stop_log"] or "[]")
    ls = con.execute("SELECT ts,sold_frac,det_ts FROM leader_sells WHERE wallet=? AND mint=?", (p["leader"], p["mint"])).fetchone()
    mirror_ts = None
    if ls and ls["sold_frac"] >= C.MIRROR_SELL_FRAC:
        # realistic accounts sell when the scanner saw the smart wallet sell; the old test 5 s after it sold
        if p["account"] in (C.ACCT_REAL, C.ACCT_REAL_SHADOW, C.ACCT_HF) and ls["det_ts"]:
            mirror_ts = max(ls["det_ts"], ls["ts"])
        else:
            mirror_ts = ls["ts"] + C.COPY_DELAY_S
    last_ts, last_px = p["last_candle_ts"], p["last_price"]
    v3 = is_v3(p)
    live_fill = v3 and p["account"] in (C.ACCT_REAL, C.ACCT_REAL_SHADOW, C.ACCT_HF)
    if v3:
        alt_step(con, p, cs, mirror_ts, t_now, live_fill)
    for (t, o, h, l, c) in cs:
        if t <= p["entry_ts"] - 60:
            continue
        kw = dict(stop=stop, peak=peak, slog=slog)
        if mirror_ts and t >= mirror_ts:
            close_position(con, p, t, o, "smart wallet sold", **kw); return
        if t >= p["entry_ts"] + C.TIME_STOP_S:
            close_position(con, p, t, o, "24h time stop", **kw); return
        if o <= stop or l <= stop:
            if v3:
                px, src = stop_px(p, t, stop, o, c, live_fill)
                con.execute("UPDATE positions SET exit_trigger_px=?, exit_fill_src=? WHERE id=?", (stop, src, p["id"]))
            else:
                px = o if o <= stop else (stop if c >= stop else c)
            close_position(con, p, t, px, stop_reason(p, stop, o <= stop), **kw); return
        if v3 and not p.get("part_ts") and h >= entry * C.HALF_AT:
            take_half(con, p, t, max(o, entry * C.HALF_AT), stop, slog, t_now); p["part_ts"] = t
        if h > peak:
            peak = h
            ns = stop_for(REAL_SPEC, entry, peak, stop, p["be_px"]) if v3 else new_stop(entry, peak, stop)
            if ns > stop:
                lab = "breakeven" if v3 and p["be_px"] and abs(ns - p["be_px"]) < 1e-9 * ns else f"peak +{(peak/entry-1)*100:.0f}%"
                slog.append([t, ns, lab]); stop = ns
        last_ts, last_px = t, c
    price_src, price_ts = "candle", ((last_ts or p["entry_ts"]) + 60)
    # Stale-candle guard: no new candle for a while (GeckoTerminal may not chart this pool at all), so check the
    # stop, the 2x half-sell, the trail and the wallet's sell on the live market price instead of a frozen one.
    q = None
    if candles_stale(last_ts, p["entry_ts"], t_now):
        e = live_quotes([p["mint"]]).get(p["mint"])
        if e and t_now - e[0] <= C.QUOTE_MAX_AGE_S:
            q = e
    if q:
        qt, qpx, qsrc = q
        kw = dict(stop=stop, peak=peak, slog=slog)
        tag = f" (live {qsrc} price, no candles)"
        if t_now >= p["entry_ts"] + C.TIME_STOP_S:
            if mirror_ts and mirror_ts < p["entry_ts"] + C.TIME_STOP_S:
                close_position(con, p, qt, qpx, "smart wallet sold" + tag, **kw)
            else:
                close_position(con, p, qt, qpx, "24h time stop" + tag, **kw)
            con.execute("UPDATE positions SET price_src=?, price_ts=? WHERE id=?", (qsrc, qt, p["id"])); return
        if mirror_ts and qt >= mirror_ts:
            close_position(con, p, qt, qpx, "smart wallet sold" + tag, **kw)
            con.execute("UPDATE positions SET price_src=?, price_ts=? WHERE id=?", (qsrc, qt, p["id"])); return
        if qpx <= stop:
            if v3:
                con.execute("UPDATE positions SET exit_trigger_px=?, exit_fill_src=? WHERE id=?", (stop, "live quote " + qsrc, p["id"]))
            close_position(con, p, qt, qpx, stop_reason(p, stop, False) + tag, **kw)
            con.execute("UPDATE positions SET price_src=?, price_ts=? WHERE id=?", (qsrc, qt, p["id"])); return
        if v3 and not p.get("part_ts") and qpx >= entry * C.HALF_AT:
            take_half(con, p, qt, qpx, stop, slog, t_now); p["part_ts"] = qt
        if qpx > peak:
            peak = qpx
            ns = stop_for(REAL_SPEC, entry, peak, stop, p["be_px"]) if v3 else new_stop(entry, peak, stop)
            if ns > stop:
                lab = "breakeven" if v3 and p["be_px"] and abs(ns - p["be_px"]) < 1e-9 * ns else f"peak +{(peak/entry-1)*100:.0f}% (live price)"
                slog.append([qt, ns, lab]); stop = ns
        last_px, price_src, price_ts = qpx, qsrc, qt
    # no candle (no trades) for a long time and no live price either -> time stop at last known price
    elif t_now >= p["entry_ts"] + C.TIME_STOP_S and (not cs or cs[-1][0] < p["entry_ts"] + C.TIME_STOP_S):
        kw = dict(stop=stop, peak=peak, slog=slog)
        if mirror_ts and mirror_ts < p["entry_ts"] + C.TIME_STOP_S:
            close_position(con, p, mirror_ts, last_px, "smart wallet sold", **kw); return
        close_position(con, p, p["entry_ts"] + C.TIME_STOP_S, last_px, "24h time stop (no price data)", **kw); return
    # last_candle_ts stays the newest real candle, so candles that turn up later are still read
    con.execute("""UPDATE positions SET stop=?,peak=?,last_candle_ts=?,last_price=?,last_update=?,stop_log=?,pool=?,
                   price_src=?,price_ts=? WHERE id=?""",
                (stop, peak, last_ts, last_px, t_now, json.dumps(slog[-30:]), pool, price_src, price_ts, p["id"]))


def guard_all(con):
    # realistic $200 account first, then the old 5 s $200 account, then the two flat-$20 tests
    opens = con.execute(f"""SELECT * FROM positions WHERE status='open' ORDER BY CASE account WHEN {C.ACCT_REAL} THEN 0
        WHEN {C.ACCT_5S} THEN 1 WHEN {C.ACCT_REAL_SHADOW} THEN 2 ELSE 3 END, last_update ASC""").fetchall()
    for k in [k for k, e in CANDLES.items() if now() - e[0] > 300]:
        CANDLES.pop(k, None)
    if not opens:
        return
    # refresh the best pool once in a while (pump.fun coins migrate from the bonding curve to PumpSwap)
    pools = {}
    stale = [p["mint"] for p in opens if now() - (p["last_update"] or 0) > 300]
    if stale:
        for m, ps in dex_pairs(set(stale)).items():
            tp = top_pair(ps)
            if tp:
                pools[m] = tp["pairAddress"]
    coins = {(pools.get(p["mint"]) or p["pool"], p["mint"]) for p in opens}
    every = max(60, len(coins) * 60 / 15)   # keep GeckoTerminal under its free limit
    due = []
    for p in opens:
        age = now() - (p["last_update"] or 0)
        if p["account"] in (C.ACCT_5S_SHADOW, C.ACCT_REAL_SHADOW) and age < every:
            continue
        if p["account"] in (C.ACCT_5S, C.ACCT_REAL, C.ACCT_HF) and age < 55:
            continue
        due.append(p)
    # live prices in one batched call for every due coin whose candles have gone stale (stale-candle guard)
    st = [p["mint"] for p in due if candles_stale(p["last_candle_ts"], p["entry_ts"], now() + 60)]
    if st:
        try:
            live_quotes(st)
        except Exception as ex:
            log.warning("live quotes error %s", ex)
    # one candle fetch per coin, from the earliest point any due position on it needs
    first = {}
    for p in due:
        k = (pools.get(p["mint"]) or p["pool"], p["mint"])
        first[k] = min(first.get(k, 1e18), candle_since(p))
    for p in due:
        k = (pools.get(p["mint"]) or p["pool"], p["mint"])
        e = CANDLES.get(k)
        if not (e and now() - e[0] < 50 and e[1] <= candle_since(p)):
            if LIM["gecko"].free() < 3:
                break
            try:
                candles_cached(k[0], k[1], first[k])
            except Exception as ex:   # carry on: update_position falls back to the live price
                log.warning("guard candles error %s %s", p["symbol"], ex)
        try:
            update_position(con, p, pools.get(p["mint"]))
            con.commit()
        except Exception as ex:
            log.warning("guard error %s %s", p["symbol"], ex)


# ----------------------------------------------------------------------------- outcome checkpoints for every signal
def checkpoints(con):
    t = now(); todo = []
    for col, sec in (("p1h", 3600), ("p6h", 6 * 3600), ("p24h", 86400)):
        rows = con.execute(f"""SELECT id,mint,price_detect FROM signals WHERE {col} IS NULL AND price_detect>0
            AND status IN ('survivor','killed') AND detected_ts<=? AND detected_ts>?""", (t - sec, t - sec - 3 * 3600)).fetchall()
        todo += [(col, r) for r in rows]
    if not todo:
        return
    pairs = dex_pairs({r["mint"] for _, r in todo})
    for col, r in todo:
        tp = top_pair(pairs.get(r["mint"]))
        pct = (float(tp["priceUsd"]) / r["price_detect"] - 1) * 100 if tp and tp.get("priceUsd") else -100.0
        con.execute(f"UPDATE signals SET {col}=? WHERE id=?", (pct, r["id"]))
    con.commit()


# ----------------------------------------------------------------------------- Scout: live feed
def record_leader_sell(con, wallet, mint, ts_sell, frac, det_ts):
    ex = con.execute("SELECT sold_frac FROM leader_sells WHERE wallet=? AND mint=?", (wallet, mint)).fetchone()
    if not ex or (ex[0] < C.MIRROR_SELL_FRAC and frac > ex[0]):
        con.execute("INSERT OR REPLACE INTO leader_sells(wallet,mint,ts,sold_frac,det_ts) VALUES(?,?,?,?,?)",
                    (wallet, mint, ts_sell, frac, det_ts))


def parse_wallet_tx(con, tx, wallet, name, t_recv, sig, open_map):
    """One of a followed wallet's transactions from the feed -> first buys (candidates) and sells of coins we hold."""
    m = tx["meta"]
    keys = [k["pubkey"] if isinstance(k, dict) else k for k in tx["transaction"]["message"]["accountKeys"]]
    bal = {}   # mint -> [pre, post, decimals, token account]
    for side, field in ((0, "preTokenBalances"), (1, "postTokenBalances")):
        for b in m.get(field) or []:
            if b.get("owner") != wallet:
                continue
            e = bal.setdefault(b["mint"], [0.0, 0.0, None, None])
            e[side] += float(b["uiTokenAmount"]["amount"]); e[2] = b["uiTokenAmount"]["decimals"]
            i = b.get("accountIndex")
            if e[3] is None and isinstance(i, int) and i < len(keys):
                e[3] = keys[i]
    block_ts = tx.get("blockTime") or t_recv
    out = []; paid = None
    for mint, (pre, post, dec, ata) in bal.items():
        if mint in C.EXCLUDE_MINTS:
            continue
        if post > pre and pre == 0:          # wallet didn't hold this coin before: a first buy
            if t_recv - SEEN_BUYS.get((wallet, mint), 0) < 600:
                continue
            SEEN_BUYS[(wallet, mint)] = t_recv
            if paid is None:
                paid = paid_something(tx, wallet)
            out.append(dict(wallet=wallet, name=name, mint=mint, ata=ata, amt=post, prev_ts=block_ts - 1, det_ts=t_recv,
                            src="stream", tx=dict(block_ts=block_ts, sig=sig, post=post, dec=dec, paid=paid)))
        elif post < pre and (wallet, mint) in open_map:   # smart wallet selling a coin we hold (mirror-sell rule)
            ref = open_map[(wallet, mint)] or pre
            frac = 1 - post / ref if ref else 1
            record_leader_sell(con, wallet, mint, block_ts, frac, t_recv)
    return out


TX_POOL = ThreadPoolExecutor(max_workers=6, thread_name_prefix="feed-tx")


TX_URLS = [C.RPC_URL, "https://solana-rpc.publicnode.com"]


def read_feed_tx(sg):
    """Read one transaction, spreading the reads over two free public nodes (the other one is tried on a 429)."""
    err = None
    for k in range(6):   # a just-confirmed transaction can take a moment to be readable
        url = TX_URLS[k % len(TX_URLS)]
        try:
            LIM["rpc"].wait()
            body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getTransaction",
                               "params": [sg, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1,
                                               "commitment": "confirmed"}]}).encode()
            req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", "User-Agent": RPC_UA})
            with urllib.request.urlopen(req, timeout=20) as r:
                raw = r.read()
            STATS["rpc_bytes"] += len(raw)
            d = json.loads(raw)
            if d.get("result"):
                return d["result"]
            if d.get("error"):
                err = d["error"]
        except Exception as e:
            err = e
        time.sleep(0.5 * (k + 1))
    if err:
        log.warning("feed tx read failed %s %s", sg[:12], str(err)[:80])
    return None


def drain_stream(con, wait_s=0.0):
    """Read the transactions the live feed queued, turn them into candidates and run them through the filters."""
    items = []
    if STREAM is not None:
        try:
            items.append(STREAM.q.get(timeout=wait_s) if wait_s > 0 else STREAM.q.get_nowait())
        except queue.Empty:
            pass
        while items and len(items) < 60:
            try:
                items.append(STREAM.q.get_nowait())
            except queue.Empty:
                break
    cands = []
    if items:
        names = {r[0]: r[1] for r in con.execute("SELECT address,name FROM wallets")}
        open_map = {(r[0], r[1]): r[2] for r in con.execute(
            "SELECT p.leader,p.mint,s.leader_post_amt FROM positions p LEFT JOIN signals s ON s.id=p.signal_id WHERE p.status='open'")}
        fresh = []
        for it in items:
            sg = it[2]
            if sg in SEEN_SIG_SET:
                continue
            if len(SEEN_SIGS) == SEEN_SIGS.maxlen:
                SEEN_SIG_SET.discard(SEEN_SIGS[0])
            SEEN_SIGS.append(sg); SEEN_SIG_SET.add(sg)
            fresh.append(it)
        txs = list(TX_POOL.map(lambda it: read_feed_tx(it[2]), fresh))   # read up to 6 transactions at once
        for (t_recv, w, sg, slot), tx in zip(fresh, txs):
            if not tx or (tx.get("meta") or {}).get("err") is not None:
                continue
            try:
                cands += parse_wallet_tx(con, tx, w, names.get(w, ""), t_recv, sg, open_map)
            except Exception as e:
                log.warning("feed tx parse failed %s %s", sg[:12], e)
        con.commit()
    if PENDING:
        due = [c for c in PENDING if now() >= c["retry_at"]]
        for c in due:
            PENDING.remove(c)
        cands += due
    if cands:
        process_new_buys(con, cands)
    for k in [k for k, t in SEEN_BUYS.items() if now() - t > 86400]:
        SEEN_BUYS.pop(k, None)
    return len(items)


# ----------------------------------------------------------------------------- Scout: holdings re-read (safety net)
def poll_wallets(con):
    t = now(); cands = []; polled = 0
    ws = con.execute("SELECT * FROM wallets WHERE active=1 ORDER BY last_poll ASC").fetchall()
    open_mints = {(r["leader"], r["mint"]) for r in con.execute("SELECT leader,mint FROM positions WHERE status='open'")}
    feed_up = STREAM is not None and STREAM.healthy()
    for w in ws:
        every = w["poll_s"] or 60
        if feed_up:
            every = max(every, C.POLL_BACKUP_S)
        if t - (w["last_poll"] or 0) < every:
            continue
        if now() - t > 50:
            break
        addr = w["address"]
        try:
            hold, n = snapshot(addr)
        except Exception as e:
            log.warning("snapshot failed %s %s", w["name"], e)
            con.execute("UPDATE wallets SET last_poll=? WHERE address=?", (now(), addr)); continue
        polled += 1; det = now()
        poll_s = 60 if n < 1500 else (180 if n < 6000 else (300 if n < 20000 else 900))
        snap = con.execute("SELECT ts,holdings FROM snapshots WHERE address=?", (addr,)).fetchone()
        if snap and det - snap["ts"] <= max(C.GAP_REBASELINE_S, poll_s * 3, (C.POLL_BACKUP_S + 120) if feed_up else 0):
            prev = json.loads(snap["holdings"])
            for mint, (amt, ata) in hold.items():
                if mint in C.EXCLUDE_MINTS:
                    continue
                if not prev.get(mint) or prev[mint][0] == 0:
                    # already taken from the live feed? then this is the same buy, not a new one
                    if det - SEEN_BUYS.get((addr, mint), 0) < 86400 or con.execute(
                            "SELECT 1 FROM signals WHERE wallet=? AND mint=? AND detected_ts>=?", (addr, mint, snap["ts"] - 120)).fetchone():
                        continue
                    SEEN_BUYS[(addr, mint)] = det
                    cands.append(dict(wallet=addr, name=w["name"], mint=mint, ata=ata, amt=amt, prev_ts=snap["ts"], det_ts=det, src="poll"))
            # leader sells on coins we hold (for the mirror-sell rule)
            for mint, (pamt, pata) in prev.items():
                if (addr, mint) not in open_mints:
                    continue
                cur = hold.get(mint, [0])[0]
                if cur < pamt:
                    sg = con.execute("SELECT leader_post_amt FROM signals WHERE wallet=? AND mint=? AND leader_post_amt>0 ORDER BY id DESC",
                                     (addr, mint)).fetchone()
                    ref = (sg and sg[0]) or pamt
                    frac = 1 - cur / ref if ref else 1
                    ts_sell = (snap["ts"] + det) / 2
                    try:
                        h = ata_history(pata, snap["ts"])
                        if h:
                            ts_sell = h[-1]["blockTime"] if cur == 0 else h[0]["blockTime"]
                    except Exception:
                        pass
                    record_leader_sell(con, addr, mint, ts_sell, frac, det)
        con.execute("INSERT OR REPLACE INTO snapshots VALUES(?,?,?)", (addr, det, json.dumps(hold)))
        con.execute("UPDATE wallets SET last_poll=?, n_accounts=?, poll_s=? WHERE address=?", (det, n, poll_s, addr))
        con.commit()
    return cands, polled


# ----------------------------------------------------------------------------- main loop
def main():
    global STREAM
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.handlers.RotatingFileHandler(C.LOG_PATH, maxBytes=5_000_000, backupCount=3),
                                  ] + ([] if os.environ.get("QUIET") else [logging.StreamHandler(sys.stdout)]))
    con = db(); migrate(con)
    if meta_get(con, "start_ts") is None:
        meta_set(con, "start_ts", now()); meta_set(con, "cash", C.START_EQUITY); con.commit()
    if meta_get(con, "real_start_ts") is None:   # realistic-entry accounts start fresh the first time this code runs
        meta_set(con, "real_start_ts", now()); meta_set(con, "cash_real", C.START_EQUITY); con.commit()
    if meta_get(con, "card_v") != 2:   # first start with the scorecard / strict 50% win-rate rules (Nov 7): rebuild now
        meta_set(con, "board_ts", 0); meta_set(con, "card_v", 2); con.commit()
    if meta_get(con, "hf_start_ts") is None:   # rug-filter shadow test account: judged on new trades only
        meta_set(con, "hf_start_ts", now()); meta_set(con, "cash_hf", C.START_EQUITY); con.commit()
    paper_reset(con)
    try:
        manual_close(con)
    except Exception as e:   # never let it stop the scanner; it is retried at the next start
        log.warning("manual close error %s", e); con.rollback()
    log.info("scanner start (PAPER MODE ONLY - no wallet, no keys, nothing signed)")
    if C.REMOVED_ERROR:
        log.warning("removed_wallets.json could not be read (%s): no wallets removed this run", C.REMOVED_ERROR)
    STREAM = streammod.WalletStream(C.WS_URLS, noisy=(meta_get(con, "feed") or {}).get("noisy") or [])
    STREAM.set_wallets([r[0] for r in con.execute("SELECT address FROM wallets WHERE active=1")])
    if not STREAM.start():
        STREAM = None
    last_eq = 0; kept0 = 0
    while True:
        loop_t = now()
        try:
            if now() - (meta_get(con, "board_ts", 0) or 0) > C.BOARD_REFRESH_S:
                try:
                    refresh_board(con)
                except Exception as e:
                    log.warning("board refresh failed %s", e); meta_set(con, "board_ts", now() - C.BOARD_REFRESH_S + 900); con.commit()
            if STREAM is not None:
                STREAM.set_wallets([r[0] for r in con.execute("SELECT address FROM wallets WHERE active=1")])
                STREAM.watch_all = {r[0] for r in con.execute("SELECT DISTINCT leader FROM positions WHERE status='open'")}
            b0 = STATS["rpc_bytes"]; tm = {}
            def _t(name, fn, *a):
                t1 = now(); r = fn(*a); tm[name] = now() - t1; return r
            _t("feed", drain_stream, con)
            cands, polled = _t("poll", poll_wallets, con)
            _t("filters", process_new_buys, con, cands)
            drain_chain_info(con)
            _t("feed2", drain_stream, con)
            _t("guard", guard_all, con)
            _t("alt exits", alt_all, con)
            live_hook(live.tick, con)
            _t("checkpoints", checkpoints, con)
            if sum(tm.values()) > 60:
                log.info("slow loop: %s", ", ".join(f"{k} {v:.0f}s" for k, v in tm.items() if v >= 5))
            if now() - last_eq > 300:
                for acct, table in ((C.ACCT_5S, "equity"), (C.ACCT_REAL, "equity_real"), (C.ACCT_HF, "equity_hf")):
                    st = account_state(con, acct)
                    con.execute(f"INSERT OR REPLACE INTO {table} VALUES(?,?,?,?,?)", (now(), st["cash"], st["open_value"], st["equity"], st["n_open"]))
                last_eq = now()
            ws_ok = 1 if (STREAM is not None and STREAM.healthy()) else 0
            kept = STREAM.n_kept if STREAM is not None else 0
            if STREAM is not None:
                meta_set(con, "feed", {"connected": ws_ok, "url": STREAM.url, "subscribed": STREAM.subscribed,
                                       "reconnects": STREAM.reconnects, "seen": STREAM.n_all, "kept": STREAM.n_kept, "skipped": STREAM.n_skipped,
                                       "last_msg": STREAM.last_msg, "error": STREAM.err, "ts": now(),
                                       "noisy": sorted(STREAM.noisy), "side": list(STREAM.side_status()),
                                       "uptime_s": STREAM.uptime_s()})
            con.execute("INSERT OR REPLACE INTO heartbeat(ts,polled,rpc_mb,note,ws,stream_n) VALUES(?,?,?,?,?,?)",
                        (now(), polled, (STATS["rpc_bytes"] - b0) / 1e6, f"{len(cands)} new buys (re-read)" if cands else None,
                         ws_ok, kept - kept0))
            kept0 = kept
            con.commit()
        except Exception as e:
            log.error("loop error %s\n%s", e, traceback.format_exc())
            try:
                con.rollback()
            except Exception:
                pass
        # between full loops, keep reading the live feed so new buys are handled within seconds
        while now() - loop_t < C.LOOP_S:
            try:
                drain_stream(con, wait_s=1.0) if STREAM is not None else time.sleep(1)
            except Exception as e:
                log.error("feed loop error %s\n%s", e, traceback.format_exc())
                try:
                    con.rollback()
                except Exception:
                    pass
                time.sleep(1)


if __name__ == "__main__":
    main()
