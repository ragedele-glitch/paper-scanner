"""Wallet scorecard and follow list for the PAPER-MODE scanner (Rage's checklist, Nov 7 2026).

Every board refresh, every wallet the scanner knows is scored on Rage's checklist, from free public data: the
wallet's own buys the scanner watched on-chain (how many, how big, what the coin did in the hour after), the
Kolscan leaderboard (its own 1-, 7- and 30-day profit and wins/losses), and the bot's own copy results (the flat $20 realistic and 5-second tests).

Each item: 1 pass, 0.5 borderline, 0 fail, None not enough data yet.
  pnl       Kolscan 7-day and 30-day profit (else the latest daily board) positive
  winrate   Kolscan wins/losses (30-day, else 7-day, else daily): 55%+ passes, 50-55% half
  quality   share of its coins that were down 25%+ an hour after it bought (the bot's max-loss stop): 50% or less
            passes, 65% or less half (needs 5 coins checked)
  exits     share of its copied coins it sold half or more of within 60 s of buying: 25% or less passes
  consist   coins up an hour after it bought: 5+ different coins passes, 2+ half; and the bot's own copies of it
            not net negative over 3+ trades
  size      median buy $500 or less passes (near the bot's $20, so its buy hasn't run the price before the copy
            lands), $2,000 or less half
  calm      the Stigman/Fox blueprint: both buy at a calm pace (Stigman 29, Fox 10 buys a day, the steady
            winners Limfork 30 and Schoen 37), while the wallets that kept losing buy 60-105 a day.
            40 or fewer buys a day passes, 60 or fewer half.

Status (what the scanner does with it):
  pinned   Rage's proven wallets (Stigman, Fox): always followed, even off the leaderboard
  removed  kept losing on the bot's copies (removed_wallets.json): never followed again until taken out of that file
  keep     net positive on the bot's copies (both flat $20 tests together), or scores 4 or more of 7 -> followed
  new      not enough data yet (fewer than 4 items measured) but positive on Kolscan -> followed so it can be scored
  bench    scores under 4 of 7 and not net positive on the bot's copies -> not followed, kept on the watchlist and
           re-scored every refresh; it is followed again as soon as it scores 4 or more
"""
import json
import re
import statistics as st
import time
import urllib.request
from collections import defaultdict

import config as C

PASS = 4.0


def kolscan_boards():
    """{address: {1|7|30: {sol, w, l}}} from the public Kolscan leaderboard page (top 50 per timeframe)."""
    out = defaultdict(dict)
    try:
        h = urllib.request.urlopen(urllib.request.Request("https://kolscan.io/leaderboard", headers={"User-Agent": "Mozilla/5.0"}),
                                   timeout=30).read().decode("utf-8", "ignore").replace('\\"', '"')
        for m in re.finditer(r'"wallet_address":"(\w+)","name":"((?:[^"\\]|\\.)*)".*?"profit":(-?[\d.e+-]+),"wins":(\d+),"losses":(\d+),"timeframe":(\d+)', h):
            out[m.group(1)][int(m.group(6))] = dict(sol=float(m.group(3)), w=int(m.group(4)), l=int(m.group(5)))
            out[m.group(1)].setdefault("name", m.group(2)[:40])
    except Exception:
        pass
    return dict(out)


def _wr(board, w):
    for tf in (30, 7, 1):
        b = board.get(tf) or board.get(str(tf))
        if b and b["w"] + b["l"] >= 10:
            return b["w"] / (b["w"] + b["l"]) * 100, tf
    if w.get("wins") is not None and (w["wins"] or 0) + (w["losses"] or 0) >= 10:
        return w["wins"] / (w["wins"] + w["losses"]) * 100, 1
    return None, None


def score_all(con, board, now=None):
    now = now or time.time()
    W = {r[0]: dict(zip(["address", "name", "active", "wins", "losses", "pnl_sol"], r))
         for r in con.execute("SELECT address,name,active,wins,losses,pnl_sol FROM wallets")}
    S = defaultdict(list)
    for r in con.execute("SELECT wallet,mint,leader_usd,p1h,detected_ts FROM signals WHERE detected_ts>?", (now - 14 * 86400,)):
        S[r[0]].append(r)
    cp = defaultdict(list)
    for r in con.execute("SELECT leader,pnl_usd FROM positions WHERE status='closed' AND account=?", (C.ACCT_REAL_SHADOW,)):
        cp[r[0]].append(r[1])
    cp5 = defaultdict(list)
    for r in con.execute("SELECT leader,pnl_usd FROM positions WHERE status='closed' AND account=?", (C.ACCT_5S_SHADOW,)):
        cp5[r[0]].append(r[1])
    dumps = defaultdict(list)
    try:
        for r in con.execute("SELECT wallet,ts,det_ts,sold_frac FROM leader_sells"):
            if r[1] and r[2]:
                dumps[r[0]].append(r[3] >= 0.5 and r[1] - r[2] <= 60)
    except Exception:
        pass
    out = {}
    for a in set(W) | set(board):
        w = W.get(a, {"name": ""}); b = board.get(a, {}); s = S.get(a, [])
        sc = {}
        p = [b[tf]["sol"] for tf in (7, 30) if tf in b] or ([w["pnl_sol"]] if w.get("pnl_sol") is not None else [])
        sc["pnl"] = None if not p else 1 if all(x > 0 for x in p) else 0.5 if any(x > 0 for x in p) else 0
        wr, wtf = _wr(b, w)
        sc["winrate"] = None if wr is None else 1 if wr >= 55 else 0.5 if wr >= 50 else 0
        p1 = [x[3] for x in s if x[3] is not None]
        dn = sum(x <= -25 for x in p1) / len(p1) * 100 if p1 else None
        sc["quality"] = None if len(p1) < 5 else 1 if dn <= 50 else 0.5 if dn <= 65 else 0
        d = dumps.get(a, [])
        sc["exits"] = None if len(d) < 3 else 1 if sum(d) / len(d) <= 0.25 else 0.5 if sum(d) / len(d) <= 0.4 else 0
        ups = len({x[1] for x in s if x[3] is not None and x[3] > 0})
        R = cp.get(a, [])
        if len(p1) < 5 and len(R) < 3:
            sc["consist"] = None
        else:
            sc["consist"] = 1 if ups >= 5 else 0.5 if ups >= 2 else 0
            if len(R) >= 3 and sum(R) < 0:
                sc["consist"] = min(sc["consist"], 0.5) if sum(x > 0 for x in R) >= 2 else 0
        usd = [x[2] for x in s if x[2]]
        med = st.median(usd) if usd else None
        sc["size"] = None if med is None else 1 if med <= 500 else 0.5 if med <= 2000 else 0
        if s:
            days = max((now - min(x[4] for x in s)) / 86400, 1.0)
            pace = len(s) / days
        else:
            pace = None
        sc["calm"] = None if pace is None or len(s) < 5 else 1 if pace <= 40 else 0.5 if pace <= 60 else 0
        items = sum(v is not None for v in sc.values())
        total = round(sum(v for v in sc.values() if v is not None), 1)
        out[a] = dict(name=w.get("name") or b.get("name") or "", score=sc, total=total, items=items,
                      facts=dict(kolscan={k: v for k, v in b.items() if k != "name"}, winrate=round(wr, 1) if wr is not None else None, winrate_tf=wtf,
                                 coins_checked=len(p1), down25_pct=round(dn) if dn is not None else None, coins_up=ups,
                                 buy_med_usd=round(med) if med else None, buys_per_day=round(pace, 1) if pace else None,
                                 copies=len(R), copy_wins=sum(x > 0 for x in R), copy_net=round(sum(R), 2),
                                 copies_5s=len(cp5.get(a, [])), copy_net_5s=round(sum(cp5.get(a, [])), 2)))
    return out


def proven(r):
    """Made money for the bot: net positive over its copies in the two flat $20 tests together."""
    f = r["facts"]
    return (f["copies"] + f["copies_5s"]) > 0 and f["copy_net"] + f["copy_net_5s"] > 0


def status(a, r):
    if a in C.PINNED_WALLETS:
        return "pinned"
    if a in C.REMOVED_WALLETS:
        return "removed"
    f = r["facts"]
    if proven(r):
        return "keep"
    if r["items"] < 4:
        return "new" if r["score"].get("pnl") == 1 else "bench"
    return "keep" if r["total"] >= PASS else "bench"
