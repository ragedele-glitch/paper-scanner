"""Wallet-score shadow test (Rage, Nov 7 evening): LOGGED ONLY, never blocks, skips or sells anything.

For every new signal it records, next to each other:
  k50   the bot's current wallet rule (the 50% win-rate bar and the other strict checks in walletcard.strict_fail),
        judged on the wallet's scorecard as it stood when the buy was seen
  s15   the profit-based score: over the wallet's last 10+ closed coins on GMGN, its winning coins made at least
        1.5x what its losing coins lost, it is in profit, and it is still in profit without its single best coin;
        the bot's other checks (losing on its copies, big buys, rug-heavy, too busy; 'proven' wallets exempt from
        rug-heavy/busy) still apply
  s18   the same with a 1.8x cutoff (on the backtest a 1.8 cutoff flipped the result, so both are watched)
  plus the wallet's profit factor, closed-coin count, profit and profit without its best coin at that moment.

No hindsight: the score only uses GMGN activity timestamped before the copied wallet's buy, and a signal is scored
only once the wallet's GMGN record has been read after that buy. 'unscored' = not enough history (fewer than 10
closed coins, or GMGN's history doesn't reach 3 days back, e.g. very busy wallets like zeropnl).

GMGN is read by one background thread at a gentle pace, cached on disk (state/gmgn/*.json.gz, kept with the bot's saved state) and refreshed
incrementally (only the newest page or two). It never runs in the buy path and uses no Helius/RPC credits.
The bot only watches the wallets it follows, so benched and watchlist wallets (Anax, 4oqL, FLCC, 32mR, ...) are
scored on the card but their buys can't be counted as trades.
"""
import gzip
import json
import logging
import os
import threading
import time
import urllib.request

import config as C

log = logging.getLogger("wscore")
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
URL = "https://gmgn.ai/vas/api/v1/wallet_activity/sol?type=buy&type=sell&wallet={w}&limit=100"
CUTS = (1.5, 1.8)
MIN_CLOSED = 10
WINDOW_D = 30
MAX_PAGES = 150           # first read of a wallet: GMGN gives 20 per page, so up to 3,000 buys/sells or 30 days (as the backtest)
PAUSE_S = 0.8             # between GMGN requests (the backtest's pace)
REFRESH_FOLLOWED_S = 1200
REFRESH_OTHER_S = 7200
GIVE_UP_S = 12 * 3600     # a signal still without GMGN data after 12 h is marked unscored (first fill can take a few hours)
MAX_WALLETS = 60
JUDGE_N = 30

COLS = ("ws_k50 INT", "ws_s15 INT", "ws_s18 INT", "ws_pf REAL", "ws_closed INT", "ws_pnl REAL", "ws_ex_best REAL",
        "ws_wr REAL", "ws_state TEXT", "ws_why TEXT")
MIGRATIONS = [f"ALTER TABLE signals ADD COLUMN {c}" for c in COLS]


def cache_dir():
    d = os.path.join(os.path.dirname(os.path.abspath(C.DB_PATH)), "gmgn")
    os.makedirs(d, exist_ok=True)
    return d


def fl(x):
    try:
        return float(x or 0)
    except Exception:
        return 0.0


# ----------------------------------------------------------------------------- GMGN cache (background thread)
class Cache:
    def __init__(self, fetch=None):
        self.lock = threading.Lock()
        self.data = {}          # wallet -> {acts: [[t, is_buy, mint, usd, buy_cost]...] oldest first, why, fetched, oldest}
        self.want = {}          # wallet -> refresh interval
        self.ok = 0; self.err = 0; self.last_ok = None; self.last_err = None
        self.fetch = fetch or self._get
        self.thread = None
        self.table = None; self.table_ts = 0
        self.retry = {}

    def _get(self, url):
        h = {"User-Agent": UA, "Accept": "application/json", "Referer": "https://gmgn.ai/"}
        with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=25) as r:
            return json.loads(r.read())

    def load(self, w):
        fn = os.path.join(cache_dir(), w + ".json.gz")
        try:
            with gzip.open(fn, "rt") as f:
                return json.load(f)
        except Exception:
            return None

    def save(self, w, o):
        fn = os.path.join(cache_dir(), w + ".json.gz")
        tmp = fn + ".tmp"
        with gzip.open(tmp, "wt") as f:
            json.dump(o, f, separators=(",", ":"))
        os.replace(tmp, fn)

    def get(self, w):
        with self.lock:
            o = self.data.get(w)
        if o is None:
            o = self.load(w)
            if o is not None:
                with self.lock:
                    self.data[w] = o
        return o

    def refresh(self, w):
        """Read the newest activity until it overlaps what is cached (first read: up to MAX_PAGES or 30 days)."""
        o = self.get(w) or {"acts": [], "why": None, "fetched": 0, "oldest": None}
        newest = o["acts"][-1][0] if o["acts"] else None
        t_start = time.time(); cut = t_start - (WINDOW_D + 1) * 86400
        got, cur, pages, why = [], "", 0, "maxpages"
        while pages < MAX_PAGES:
            d = self.fetch(URL.format(w=w) + (f"&cursor={cur}" if cur else ""))
            pages += 1
            if not d or d.get("code") not in (0, None) or not isinstance(d.get("data"), dict):
                raise RuntimeError(f"GMGN answered {str(d)[:80]}")
            a = d["data"].get("activities") or []
            for x in a:
                e = x.get("event_type")
                if e not in ("buy", "sell"):
                    continue
                got.append([int(x["timestamp"]), 1 if e == "buy" else 0, x["token"]["address"], fl(x.get("cost_usd")),
                            fl(x.get("buy_cost_usd"))])
            cur = d["data"].get("next")
            if not a or not cur:
                why = "end"; break
            last = int(a[-1]["timestamp"])
            if newest is not None and last < newest:
                why = "overlap"; break
            if last < cut:
                why = "cutoff"; break
            time.sleep(PAUSE_S)
        seen = {tuple(x) for x in o["acts"]}
        acts = o["acts"] + [x for x in got if tuple(x) not in seen]
        acts.sort(key=lambda x: x[0])
        acts = [x for x in acts if x[0] >= cut]
        if newest is None:
            o["why"] = why                      # how far back the first read reached
            o["oldest"] = acts[0][0] if acts else None
            if why == "end":
                o["oldest"] = 0                 # the wallet's whole history is in
        elif why == "maxpages":
            # a gap opened between the new pages and the old cache (very busy wallet): keep only the new run
            acts = sorted(got, key=lambda x: x[0]); acts = [x for x in acts if x[0] >= cut]
            o["why"] = "maxpages"; o["oldest"] = acts[0][0] if acts else None
        if o.get("oldest") not in (None, 0) and o["oldest"] < cut:
            o["oldest"] = cut
        o["acts"] = acts; o["fetched"] = t_start
        self.save(w, o)
        with self.lock:
            self.data[w] = o
        return o

    def set_wanted(self, want):
        with self.lock:
            self.want = dict(want)

    def due(self):
        with self.lock:
            want = dict(self.want)
        best = None
        for w, every in want.items():
            if self.retry.get(w, 0) > time.time():
                continue
            o = self.get(w)
            age = time.time() - (o["fetched"] if o else 0)
            if age >= every:
                k = (every, -age)
                if best is None or k < best[0]:
                    best = (k, w)
        return best and best[1]

    def make_table(self):
        """The card's wallet table, built here (own read-only DB connection) so the main loop never waits on it."""
        import sqlite3
        try:
            c = sqlite3.connect(f"file:{C.DB_PATH}?mode=ro", uri=True, timeout=30)
            try:
                self.table = wallet_table(c)
            finally:
                c.close()
        except Exception as e:
            log.info("wallet-score table failed %s", e)
        self.table_ts = time.time()

    def run(self):
        while True:
            w = None
            try:
                if time.time() - self.table_ts > 600:
                    self.make_table()
                w = self.due()
                if not w:
                    time.sleep(20); continue
                self.refresh(w)
                self.ok += 1; self.last_ok = time.time()
            except Exception as e:
                self.err += 1; self.last_err = f"{type(e).__name__}: {e}"[:160]
                log.info("wallet-score GMGN read failed %s %s", (w or "")[:6], self.last_err)
                if w:   # don't hammer: try this wallet again in 10 minutes (its cached record is left as it was)
                    self.retry[w] = time.time() + 600
                time.sleep(30)
            time.sleep(PAUSE_S)

    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self.run, name="wscore-gmgn", daemon=True)
            self.thread.start()


CACHE = Cache()


# ----------------------------------------------------------------------------- the score (same maths as the backtest)
def gstats(o, t0, days=WINDOW_D):
    """Wallet's GMGN record from activity strictly before t0. None = no data; {'short': True} = history too short."""
    if not o or not o.get("acts"):
        return None
    oldest = o.get("oldest")
    if oldest is None:
        return None
    if oldest > t0 - 3 * 86400:
        return dict(short=True)
    lo = max(t0 - days * 86400, oldest)
    tok = {}
    for t, buy, m, usd, bc in o["acts"]:
        if t >= t0:
            break
        k = tok.setdefault(m, dict(fb=None, buy=0.0, nb=0, ns=0, sold=0.0, cb=0.0))
        if buy:
            if k["fb"] is None:
                k["fb"] = t
            k["nb"] += 1; k["buy"] += usd
        else:
            k["ns"] += 1; k["sold"] += usd; k["cb"] += bc
    op = {m: k for m, k in tok.items() if k["fb"] and k["fb"] >= lo}
    cl = [k for k in op.values() if k["ns"] and k["cb"] > 0 and k["cb"] >= 0.8 * k["buy"]]
    span = max((t0 - lo) / 86400, 0.5)
    r = dict(span=span, coins=len(op), closed=len(cl), pace=len(op) / span)
    if cl:
        pnl = [k["sold"] - k["cb"] for k in cl]
        g = sum(p for p in pnl if p > 0); l_ = -sum(p for p in pnl if p < 0)
        r.update(wr=sum(p > 0 for p in pnl) / len(pnl), pnl=sum(pnl), pf=(g / l_ if l_ > 0 else 99.0),
                 pnl_ex_best=sum(pnl) - max(pnl))
    return r


def score_gate(g, cut):
    """None if the profit score passes at this cutoff, else the reason."""
    if g is None:
        return "no GMGN data"
    if g.get("short"):
        return "GMGN history too short (under 3 days reachable)"
    if g.get("closed", 0) < MIN_CLOSED:
        return f"only {g.get('closed', 0)} closed coins on GMGN (needs {MIN_CLOSED})"
    if g["pnl"] <= 0:
        return "losing on GMGN"
    if g["pnl_ex_best"] <= 0:
        return "in the red without its best coin"
    if g["pf"] < cut:
        return f"profit factor {g['pf']:.2f} under {cut}"
    return None


def other_fail(r):
    """The bot's other strict checks (walletcard.strict_fail without the win-rate / Kolscan-profit lines)."""
    if not r:
        return None
    import walletcard as WC
    f, sc = r.get("facts") or {}, r.get("score") or {}
    n = (f.get("copies") or 0) + (f.get("copies_5s") or 0)
    if n >= 3 and (f.get("copy_net") or 0) + (f.get("copy_net_5s") or 0) < 0:
        return "losing on the bot's copies"
    if sc.get("size") == 0:
        return "buys too big (median over $2,000)"
    try:
        if WC.proven(r):
            return None
    except Exception:
        pass
    if f.get("down25_pct") is not None and (f.get("coins_checked") or 0) >= 5 and f["down25_pct"] > WC.RUG_HEAVY:
        return f"rug-heavy ({f['down25_pct']}% of its coins down 25%+ an hour after it bought)"
    if sc.get("calm") is not None and (f.get("buys_per_day") or 0) > 40:
        return f"too busy ({f['buys_per_day']} buys a day)"
    return None


def k50_gate(r):
    """The bot's current rule: the 50% win-rate bar plus the other strict checks."""
    if not r:
        return "no scorecard yet"
    import walletcard as WC
    try:
        return WC.strict_fail(r)
    except Exception as e:
        return f"scorecard unreadable ({type(e).__name__})"


def card_of(con, wallet):
    row = con.execute("SELECT card_json FROM wallets WHERE address=?", (wallet,)).fetchone()
    try:
        return json.loads(row[0]) if row and row[0] else None
    except Exception:
        return None


def judge(o, t0, card):
    g = gstats(o, t0)
    oth = other_fail(card)
    out = {"g": g, "k50": k50_gate(card), "other": oth}
    for cut in CUTS:
        out[cut] = score_gate(g, cut) or oth
    out["unscored"] = unscored_reason(g)
    return out


def unscored_reason(g):
    """Why a wallet can't be scored (not enough GMGN history), or None if it can."""
    if g and not g.get("short") and g.get("closed", 0) >= MIN_CLOSED and "pnl" in g:
        return None
    return score_gate(g, 0.0) or "not enough GMGN history"


# ----------------------------------------------------------------------------- main-loop hooks (cheap, no network)
def wanted(con):
    """Wallets to keep a GMGN record for: followed + pinned first, then watchlist and benched wallets the bot copied."""
    want = {}
    for (a,) in con.execute("SELECT address FROM wallets WHERE active=1"):
        want[a] = REFRESH_FOLLOWED_S
    for a in C.PINNED_WALLETS:
        want[a] = REFRESH_FOLLOWED_S
    try:
        v = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "vetted_wallets.json")))
        for x in v.get("watch") or []:
            if x.get("address"):
                want.setdefault(x["address"], REFRESH_OTHER_S)
    except Exception:
        pass
    rows = con.execute("SELECT w.address, COUNT(p.id) n FROM wallets w JOIN positions p ON p.leader=w.address "
                       "WHERE w.card_status='bench' GROUP BY w.address ORDER BY n DESC").fetchall()
    for a, _ in rows:
        if len(want) >= MAX_WALLETS:
            break
        want.setdefault(a, REFRESH_OTHER_S)
    return want


_last_want = [0.0]
_last_wall = [0.0]
_last_tick = [0.0]


def meta_get(con, k, default=None):
    r = con.execute("SELECT value FROM meta WHERE key=?", (k,)).fetchone()
    return json.loads(r[0]) if r else default


def meta_set(con, k, v):
    con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (k, json.dumps(v)))


def start(con):
    if meta_get(con, "ws_start_ts") is None:
        meta_set(con, "ws_start_ts", time.time()); con.commit()
    CACHE.set_wanted(wanted(con)); _last_want[0] = time.time()
    CACHE.start()


def tick(con):
    """Score new signals whose wallet's GMGN record has been read after the buy. Never blocks anything."""
    since = meta_get(con, "ws_start_ts")
    if since is None:
        return
    t = time.time()
    if t - _last_tick[0] < 5:
        return
    _last_tick[0] = t
    if t - _last_want[0] > 600:
        CACHE.set_wanted(wanted(con)); _last_want[0] = t
    rows = con.execute("SELECT id, wallet, COALESCE(leader_buy_ts, detected_ts), detected_ts FROM signals "
                       "WHERE detected_ts>=? AND ws_state IS NULL ORDER BY id", (since,)).fetchall()
    cards = {}; done = 0
    for sid, w, t0, det in rows:
        o = CACHE.get(w)
        if not o or (o.get("fetched") or 0) <= t0:
            if t - det < GIVE_UP_S:
                continue          # wait until the wallet's GMGN record has been read after this buy
            o = o if o and o.get("acts") else None
        if done >= 40:
            break                 # spread big backlogs over several loops (keeps each loop to a few ms)
        done += 1
        if w not in cards:
            cards[w] = card_of(con, w)
        j = judge(o, t0, cards[w])
        g = j["g"] or {}
        un = j["unscored"]
        state = "unscored" if un else "scored"
        why = {"k50": j["k50"], "s15": j[1.5], "s18": j[1.8], "unscored": un}
        con.execute("UPDATE signals SET ws_k50=?, ws_s15=?, ws_s18=?, ws_pf=?, ws_closed=?, ws_pnl=?, ws_ex_best=?, ws_wr=?, "
                    "ws_state=?, ws_why=? WHERE id=?",
                    (0 if j["k50"] else 1, None if un else (0 if j[1.5] else 1), None if un else (0 if j[1.8] else 1),
                     g.get("pf"), g.get("closed"), g.get("pnl"), g.get("pnl_ex_best"),
                     None if g.get("wr") is None else round(g["wr"] * 100, 1), state, json.dumps(why), sid))
    if done:
        con.commit()
    if t - _last_wall[0] > 300 and CACHE.table is not None:
        _last_wall[0] = t
        try:
            meta_set(con, "ws_wallets", CACHE.table); meta_set(con, "ws_gmgn", {
                "ok": CACHE.ok, "err": CACHE.err, "last_ok": CACHE.last_ok, "last_err": CACHE.last_err,
                "wanted": len(CACHE.want), "read": sum(1 for w in CACHE.want if CACHE.get(w)), "ts": t})
            con.commit()
        except Exception as e:
            log.info("wallet-score table failed %s", e)


def wallet_table(con):
    """Each tracked wallet's score right now (for the card): followed, pinned, watchlist and benched."""
    t = time.time(); out = []
    names = {a: (n, s, act) for a, n, s, act in con.execute("SELECT address, name, card_status, active FROM wallets")}
    try:
        v = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "vetted_wallets.json")))
        watch = {x["address"]: x.get("name") for x in v.get("watch") or [] if x.get("address")}
    except Exception:
        watch = {}
    for w in list(CACHE.want):
        nm, st, act = names.get(w, (watch.get(w) or w[:6], None, 0))
        grp = "pinned" if w in C.PINNED_WALLETS else "followed" if act else "watchlist" if w in watch else "benched"
        o = CACHE.get(w)
        if not o:
            out.append(dict(name=watch.get(w) or nm, group=grp, state="not read yet")); continue
        card = card_of(con, w)
        j = judge(o, t, card)
        g = j["g"] or {}
        un = j["unscored"]
        out.append(dict(name=watch.get(w) or nm, group=grp, state="unscored" if un else "scored", why_un=un,
                        pf=g.get("pf"), closed=g.get("closed"), pnl=g.get("pnl"), ex_best=g.get("pnl_ex_best"),
                        wr=None if g.get("wr") is None else round(g["wr"] * 100), pace=g.get("pace"), days=g.get("span"),
                        s15=None if un else j[1.5] is None, s18=None if un else j[1.8] is None,
                        k50=j["k50"] is None, why15=j[1.5], why18=j[1.8], why50=j["k50"]))
    order = {"followed": 0, "pinned": 0, "watchlist": 1, "benched": 2}
    out.sort(key=lambda x: (order[x["group"]], -(x.get("pf") or 0)))
    return out


# ----------------------------------------------------------------------------- dashboard card and report (read-only)
RULES = [("all", "Every trade the bot took"), ("ws_k50", "Current rule (50% win rate + other checks)"),
         ("ws_s15", "Profit score, 1.5x cutoff"), ("ws_s18", "Profit score, 1.8x cutoff")]
BUSY_NOTE = ("GMGN shows Kadenox, Limfork and Schoen buying 63-68 coins a day, over the 40-a-day limit. Not removed: "
             "the bot's busy check uses the buys it sees itself, and these three are still followed.")


def view(con):
    def mg(k):
        r = con.execute("SELECT value FROM meta WHERE key=?", (k,)).fetchone()
        return json.loads(r[0]) if r else None
    since = mg("ws_start_ts")
    out = {"since": since, "judge_n": JUDGE_N, "cuts": list(CUTS), "rules": [{"key": k, "label": l} for k, l in RULES],
           "busy_note": BUSY_NOTE, "accounts": [], "recent": []}
    if since is None:
        out["pending"] = True
        return out
    for acct, lab in ((C.ACCT_REAL, "Main $200 account"), (C.ACCT_REAL_SHADOW, "Flat-$20 test (every survivor)")):
        rows = con.execute("SELECT p.symbol, p.mint, p.status, p.pnl_usd, p.entry_ts, s.wallet_name, s.ws_k50, s.ws_s15, "
                           "s.ws_s18, s.ws_state, s.ws_pf, s.ws_closed FROM positions p JOIN signals s ON s.id=p.signal_id "
                           "WHERE p.account=? AND p.entry_ts>=?", (acct, since)).fetchall()
        a = {"label": lab, "trades": len(rows), "pending": sum(1 for r in rows if r[9] is None),
             "unscored": sum(1 for r in rows if r[9] == "unscored"), "rules": {}}
        for i, (k, _) in enumerate(RULES):
            sel = rows if k == "all" else [r for r in rows if r[6 + i - 1] == 1]
            cl = [r for r in sel if r[2] == "closed"]
            a["rules"][k] = {"trades": len(sel), "closed": len(cl), "open": len(sel) - len(cl),
                             "wins": sum(1 for r in cl if (r[3] or 0) > 0), "losses": sum(1 for r in cl if (r[3] or 0) <= 0),
                             "pnl": round(sum(r[3] or 0 for r in cl), 2)}
        a["closed"] = a["rules"]["all"]["closed"]
        out["accounts"].append(a)
        if acct == C.ACCT_REAL:
            for r in sorted(rows, key=lambda r: -(r[4] or 0))[:10]:
                out["recent"].append({"coin": r[0] or r[1][:6], "wallet": r[5], "status": r[2], "pnl": r[3], "k50": r[6],
                                      "s15": r[7], "s18": r[8], "state": r[9], "pf": r[10], "closed": r[11]})
    out["logged"] = con.execute("SELECT COUNT(*) FROM signals WHERE detected_ts>=? AND ws_state IS NOT NULL", (since,)).fetchone()[0]
    out["wallets"] = mg("ws_wallets") or []
    out["gmgn"] = mg("ws_gmgn")
    return out


def report_lines(con):
    v = view(con)
    if v.get("pending"):
        return ["WALLET SCORE SHADOW TEST (logged only, never blocks): starts at the next scanner start.", "  " + BUSY_NOTE]
    m = v["accounts"][0]
    L = [f"WALLET SCORE SHADOW TEST (logged only, never blocks; new trades only: {m['closed']} closed main-account trades so far, "
         f"judge after about {v['judge_n']}; {m['unscored']} unscored, {m['pending']} waiting for GMGN):"]
    for a in v["accounts"]:
        parts = []
        for k, lab in RULES:
            g = a["rules"][k]
            parts.append(f"{lab}: {g['trades']} trades ({g['wins']} won, {g['losses']} lost" + (f", {g['open']} open" if g["open"] else "")
                         + f") ${g['pnl']:+.2f}")
        L.append(f"  {a['label']}: " + "; ".join(parts))
    L.append("  " + BUSY_NOTE)
    return L
