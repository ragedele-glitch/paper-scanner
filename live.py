"""LIVE MODE for the copy-trade scanner: real swaps through Jupiter on Solana.

SWITCHED OFF BY DEFAULT. With LIVE_MODE unset or "off" every function here returns at once and makes no network call,
so the paper scanner runs exactly as before.

Modes (the LIVE_MODE setting):
  off      default. Nothing happens.
  dry-run  runs every live check and asks Jupiter for real swap quotes (read-only GET requests), then records what a
           real trade would have got. It never loads a key, never builds, signs or sends a transaction.
  live     real money. Signs Jupiter swap transactions with the wallet key and sends them to Solana.

There is no key in this code or in the repository. In live mode the key is read from the LIVE_WALLET_KEY environment
variable, which only ever comes from a secret the owner adds himself (a GitHub Actions secret or a server env file).
The key is never written to disk, never logged and never printed.

Live mode copies the paper account: when the paper account opens a trade, live buys the same coin with a small fixed
size; when the paper account closes that trade (stop, trailing stop, smart wallet sold, 24 h time stop), live sells
every token of that coin it holds. Safety rails (all settings are required; live refuses to start if one is missing):
  LIVE_MODE                  off | dry-run | live
  LIVE_WALLET_ADDRESS        public address of the fresh hot wallet (the key must match it). Optional in dry-run.
  LIVE_TRADE_USD             dollars per trade (hard cap 50)
  LIVE_TOTAL_BUDGET_USD      open trades + all realised losses can never go past this (hard cap 100)
  LIVE_MAX_OPEN              most live trades open at once (hard cap 5)
  LIVE_DAILY_LOSS_USD        no new trades for the rest of the day (Lagos time) once realised losses reach this
  LIVE_SLIPPAGE_BPS          slippage limit sent to Jupiter, in basis points: 300 = 3% (hard cap 1500)
  LIVE_MAX_PRICE_IMPACT_PCT  skip a buy if Jupiter's quote says the trade moves the price more than this
  LIVE_MAX_SIGNAL_AGE_S      skip a buy if the smart wallet bought more than this many seconds ago
  LIVE_MAX_WALLET_USD        refuse to trade at all if the wallet holds more than this (keeps it to a small hot wallet)
  LIVE_KILL                  0 normally. 1 = kill switch: open nothing, sell every coin in the wallet back to SOL.
Fast kill switch while it runs: set "kill": true in live_control.json in the repository (editable from the GitHub app).
The bot re-reads that file every minute; if it can't read it for 10 minutes it stops opening new trades.
Secrets (never in the code): LIVE_WALLET_KEY (live only), optional LIVE_RPC_URL (a private Solana RPC), optional
JUP_API_KEY (Jupiter's free key gives a higher rate limit than keyless access).
"""
import base64, json, logging, os, time, urllib.error, urllib.request

import config as C

log = logging.getLogger("scanner.live")

SOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
LAMPORTS = 1_000_000_000

# Hard limits in the code. A setting above one of these makes live mode refuse to start.
HARD_MAX_TRADE_USD = 50.0
HARD_MAX_BUDGET_USD = 100.0
HARD_MAX_OPEN = 5
HARD_MAX_SLIPPAGE_BPS = 1500
FEE_RESERVE_SOL = 0.02          # always left in the wallet for network fees and token-account rent
PRIORITY_FEE_MAX_LAMPORTS = 200_000   # at most 0.0002 SOL priority fee per swap
CONFIRM_TIMEOUT_S = 75
LAGOS_UTC_OFFSET_S = 3600

REQUIRED = ["LIVE_TRADE_USD", "LIVE_TOTAL_BUDGET_USD", "LIVE_MAX_OPEN", "LIVE_DAILY_LOSS_USD", "LIVE_SLIPPAGE_BPS",
            "LIVE_MAX_PRICE_IMPACT_PCT", "LIVE_MAX_SIGNAL_AGE_S", "LIVE_MAX_WALLET_USD", "LIVE_KILL"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS live_trades(id INTEGER PRIMARY KEY, mode TEXT, signal_id INT, mint TEXT, symbol TEXT,
  opened_ts REAL, buy_sig TEXT, sol_in REAL, usd_in REAL, token_raw TEXT, price_impact_pct REAL, status TEXT,
  closed_ts REAL, sell_sig TEXT, sol_out REAL, usd_out REAL, pnl_usd REAL, close_reason TEXT, sell_tries INT DEFAULT 0,
  notes TEXT);
CREATE TABLE IF NOT EXISTS live_events(ts REAL, kind TEXT, msg TEXT);
"""

CONTROL_FILE = "live_control.json"
CONTROL_STALE_S = 600

_state = {"checked": None, "control": {"kill": False}, "control_ok": 0.0, "control_tried": 0.0, "settings": None, "problems": [], "keypair": None, "sol_px": (0, 0.0),
          "jup_last": 0.0, "wallet_too_big": False}


# ----------------------------------------------------------------------------- settings
def mode():
    m = (os.environ.get("LIVE_MODE") or "off").strip().lower()
    return m if m in ("off", "dry-run", "live") else "invalid"


def load_settings():
    """Returns (settings, problems). Live does nothing unless problems is empty."""
    m = mode()
    if m == "off":
        return None, []
    if m == "invalid":
        return None, ["LIVE_MODE must be off, dry-run or live"]
    s = {"mode": m}; problems = []
    for k in REQUIRED:
        v = (os.environ.get(k) or "").strip()
        if v == "":
            problems.append(f"{k} is missing"); continue
        try:
            s[k] = float(v)
        except ValueError:
            problems.append(f"{k} is not a number")
    if problems:
        return None, problems
    if not 0 < s["LIVE_TRADE_USD"] <= HARD_MAX_TRADE_USD:
        problems.append(f"LIVE_TRADE_USD must be above 0 and at most {HARD_MAX_TRADE_USD:.0f}")
    if not 0 < s["LIVE_TOTAL_BUDGET_USD"] <= HARD_MAX_BUDGET_USD:
        problems.append(f"LIVE_TOTAL_BUDGET_USD must be above 0 and at most {HARD_MAX_BUDGET_USD:.0f}")
    if s["LIVE_TRADE_USD"] > s["LIVE_TOTAL_BUDGET_USD"]:
        problems.append("LIVE_TRADE_USD can't be bigger than LIVE_TOTAL_BUDGET_USD")
    if not (1 <= s["LIVE_MAX_OPEN"] <= HARD_MAX_OPEN and s["LIVE_MAX_OPEN"] == int(s["LIVE_MAX_OPEN"])):
        problems.append(f"LIVE_MAX_OPEN must be a whole number from 1 to {HARD_MAX_OPEN}")
    if not 0 < s["LIVE_DAILY_LOSS_USD"] <= s["LIVE_TOTAL_BUDGET_USD"]:
        problems.append("LIVE_DAILY_LOSS_USD must be above 0 and no bigger than the total budget")
    if not 10 <= s["LIVE_SLIPPAGE_BPS"] <= HARD_MAX_SLIPPAGE_BPS:
        problems.append(f"LIVE_SLIPPAGE_BPS must be from 10 to {HARD_MAX_SLIPPAGE_BPS}")
    if not 0 < s["LIVE_MAX_PRICE_IMPACT_PCT"] <= 15:
        problems.append("LIVE_MAX_PRICE_IMPACT_PCT must be above 0 and at most 15")
    if not 5 <= s["LIVE_MAX_SIGNAL_AGE_S"] <= 600:
        problems.append("LIVE_MAX_SIGNAL_AGE_S must be from 5 to 600")
    if not 0 < s["LIVE_MAX_WALLET_USD"] <= 2 * HARD_MAX_BUDGET_USD:
        problems.append(f"LIVE_MAX_WALLET_USD must be above 0 and at most {2 * HARD_MAX_BUDGET_USD:.0f}")
    if s["LIVE_KILL"] not in (0, 1):
        problems.append("LIVE_KILL must be 0 or 1")
    addr = (os.environ.get("LIVE_WALLET_ADDRESS") or "").strip()
    s["address"] = addr or None
    if m == "live":
        if not addr:
            problems.append("LIVE_WALLET_ADDRESS is missing")
        if not (os.environ.get("LIVE_WALLET_KEY") or "").strip():
            problems.append("LIVE_WALLET_KEY secret is missing")
    s["rpc"] = (os.environ.get("LIVE_RPC_URL") or "").strip() or C.RPC_URL
    s["jup_key"] = (os.environ.get("JUP_API_KEY") or "").strip() or None
    return (None if problems else s), problems


def _load_keypair(address):
    """Live mode only. Reads the key from the environment, checks it matches the wallet address, keeps it in memory."""
    raw = os.environ.pop("LIVE_WALLET_KEY", "").strip()   # removed from the environment once read
    try:
        from solders.keypair import Keypair
    except ImportError:
        raise RuntimeError("the solders package is not installed (pip install solders)") from None
    try:
        kp = Keypair.from_bytes(bytes(json.loads(raw))) if raw.startswith("[") else Keypair.from_base58_string(raw)
    except Exception:
        raise RuntimeError("LIVE_WALLET_KEY could not be read (expected a base58 private key or a JSON byte list)") from None
    finally:
        raw = None
    if str(kp.pubkey()) != address:
        raise RuntimeError("LIVE_WALLET_KEY does not belong to LIVE_WALLET_ADDRESS")
    return kp


def ready(con):
    """Checks settings once per process. Returns the settings when live or dry-run may act, else None."""
    if _state["checked"] is None:
        s, problems = load_settings()
        if s and s["mode"] == "live":
            try:
                _state["keypair"] = _load_keypair(s["address"])
            except Exception as e:
                problems = [str(e)]; s = None
        os.environ.pop("LIVE_WALLET_KEY", None)
        _state.update(checked=time.time(), settings=s, problems=problems)
        if mode() != "off":
            con.executescript(SCHEMA)
            if problems:
                event(con, "refused", f"live mode ({mode()}) refused to start: " + "; ".join(problems))
            else:
                event(con, "start", f"live mode started: {s['mode']}, ${s['LIVE_TRADE_USD']:.2f} a trade, "
                                    f"budget ${s['LIVE_TOTAL_BUDGET_USD']:.2f}, max {int(s['LIVE_MAX_OPEN'])} open, "
                                    f"daily loss limit ${s['LIVE_DAILY_LOSS_USD']:.2f}, slippage {s['LIVE_SLIPPAGE_BPS']:.0f} bps")
            con.commit()
    return _state["settings"]


def event(con, kind, msg):
    con.execute("INSERT INTO live_events VALUES(?,?,?)", (time.time(), kind, msg[:500]))
    (log.warning if kind in ("refused", "error", "kill", "blocked") else log.info)("LIVE %s: %s", kind, msg)


# ----------------------------------------------------------------------------- network
UA = "memecoin-scanner-live/1.0 (python-urllib)"


def _http(url, body=None, headers=None, timeout=30):
    h = {"User-Agent": UA, "Accept": "application/json"}
    if body is not None:
        h["Content-Type"] = "application/json"
    h.update(headers or {})
    data = json.dumps(body).encode() if body is not None else None
    for i in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=h), timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and i < 2:
                time.sleep(3 * (i + 1)); continue
            detail = ""
            try:
                detail = e.read().decode("utf-8", "ignore")[:200]
            except Exception:
                pass
            raise RuntimeError(f"HTTP {e.code} from {url.split('?')[0]} {detail}") from None
        except (urllib.error.URLError, TimeoutError):
            if i < 2:
                time.sleep(2 * (i + 1)); continue
            raise


def _jup(s, path, params=None, body=None):
    # keyless Jupiter access allows about one request every 2 seconds; a free API key allows one a second
    gap = 1.1 if s["jup_key"] else 2.2
    wait = _state["jup_last"] + gap - time.time()
    if wait > 0:
        time.sleep(wait)
    _state["jup_last"] = time.time()
    base = "https://api.jup.ag" if s["jup_key"] else "https://lite-api.jup.ag"
    url = base + path + ("?" + "&".join(f"{k}={v}" for k, v in params.items()) if params else "")
    return _http(url, body, {"x-api-key": s["jup_key"]} if s["jup_key"] else None)


def _rpc(s, method, params):
    j = _http(s["rpc"], {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=60)
    if "error" in j:
        raise RuntimeError(f"rpc {method}: {j['error']}")
    return j["result"]


def quote(s, input_mint, output_mint, amount_raw):
    q = _jup(s, "/swap/v1/quote", {"inputMint": input_mint, "outputMint": output_mint, "amount": int(amount_raw),
                                   "slippageBps": int(s["LIVE_SLIPPAGE_BPS"]), "swapMode": "ExactIn"})
    if not q or not q.get("outAmount") or not q.get("routePlan"):
        raise RuntimeError("Jupiter found no route")
    return q


def sol_usd(s):
    t, px = _state["sol_px"]
    if time.time() - t < 60 and px > 0:
        return px
    q = quote(s, SOL, USDC, LAMPORTS)
    px = int(q["outAmount"]) / 1e6
    _state["sol_px"] = (time.time(), px)
    return px


def impact_pct(q):
    return abs(float(q.get("priceImpactPct") or 0)) * 100   # Jupiter gives a fraction: 0.01 = 1%


def sol_balance(s):
    return _rpc(s, "getBalance", [s["address"], {"commitment": "confirmed"}])["value"] / LAMPORTS


def token_balances(s, mint=None):
    """{mint: raw amount} for the wallet; one mint, or every coin (both token programs) when mint is None."""
    out = {}
    filters = [{"mint": mint}] if mint else [{"programId": p} for p in C.TOKEN_PROGRAMS]
    for f in filters:
        res = _rpc(s, "getTokenAccountsByOwner", [s["address"], f, {"encoding": "jsonParsed", "commitment": "confirmed"}])
        for a in res["value"]:
            info = a["account"]["data"]["parsed"]["info"]
            amt = int(info["tokenAmount"]["amount"])
            if amt:
                out[info["mint"]] = out.get(info["mint"], 0) + amt
    return out


# ----------------------------------------------------------------------------- signing and sending (live mode only)
def _swap_and_send(s, q):
    """Builds the swap with Jupiter, checks it, signs it with the wallet key and sends it. Returns the signature."""
    if s["mode"] != "live" or _state["keypair"] is None:
        raise RuntimeError("refusing to sign: not in live mode")
    from solders.transaction import VersionedTransaction
    kp = _state["keypair"]
    body = {"quoteResponse": q, "userPublicKey": s["address"], "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "prioritizationFeeLamports": {"priorityLevelWithMaxLamports": {"maxLamports": PRIORITY_FEE_MAX_LAMPORTS,
                                                                           "priorityLevel": "veryHigh"}}}
    r = _jup(s, "/swap/v1/swap", body=body)
    tx = VersionedTransaction.from_bytes(base64.b64decode(r["swapTransaction"]))
    msg = tx.message
    if msg.header.num_required_signatures != 1 or str(msg.account_keys[0]) != s["address"]:
        raise RuntimeError("Jupiter's transaction doesn't have our wallet as its only signer; not signed")
    signed = VersionedTransaction(msg, [kp])
    sig = _rpc(s, "sendTransaction", [base64.b64encode(bytes(signed)).decode(),
                                      {"encoding": "base64", "skipPreflight": False,
                                       "preflightCommitment": "confirmed", "maxRetries": 3}])
    t0 = time.time()
    while time.time() - t0 < CONFIRM_TIMEOUT_S:
        time.sleep(2)
        st = _rpc(s, "getSignatureStatuses", [[sig]])["value"][0]
        if st and st.get("err"):
            raise RuntimeError(f"transaction failed on-chain: {st['err']}")
        if st and st.get("confirmationStatus") in ("confirmed", "finalized"):
            return sig
    raise RuntimeError(f"transaction {sig} not confirmed after {CONFIRM_TIMEOUT_S}s")


# ----------------------------------------------------------------------------- kill switch file
def read_control():
    """live_control.json, re-read at most once a minute: from the GitHub API inside Actions, else the local file."""
    if time.time() - _state["control_tried"] < 60:
        return _state["control"]
    _state["control_tried"] = time.time()
    try:
        repo, token = os.environ.get("GITHUB_REPOSITORY"), os.environ.get("GITHUB_TOKEN")
        if repo and token:
            j = _http(f"https://api.github.com/repos/{repo}/contents/{CONTROL_FILE}?ref=main",
                      headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}, timeout=20)
            ctl = json.loads(base64.b64decode(j["content"]))
        else:
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), CONTROL_FILE)) as f:
                ctl = json.load(f)
        _state["control"] = {"kill": ctl.get("kill") is True}
        _state["control_ok"] = time.time()
    except Exception as e:
        log.warning("LIVE could not read %s: %s", CONTROL_FILE, type(e).__name__)
    return _state["control"]


def kill_on(s):
    return s["LIVE_KILL"] == 1 or read_control()["kill"]


# ----------------------------------------------------------------------------- rails
def day_start(t=None):
    t = time.time() if t is None else t
    return (t + LAGOS_UTC_OFFSET_S) // 86400 * 86400 - LAGOS_UTC_OFFSET_S


def book(con, m):
    """Open exposure, realised loss today and all-time, for one mode (live and dry-run keep separate books)."""
    q = lambda sql, a=(): con.execute(sql, a).fetchone()[0] or 0.0
    open_n = q("SELECT COUNT(*) FROM live_trades WHERE mode=? AND status IN ('open','sell_failed')", (m,))
    exposure = q("SELECT SUM(usd_in) FROM live_trades WHERE mode=? AND status IN ('open','sell_failed')", (m,))
    pnl_all = q("SELECT SUM(pnl_usd) FROM live_trades WHERE mode=? AND status='closed'", (m,))
    pnl_day = q("SELECT SUM(pnl_usd) FROM live_trades WHERE mode=? AND status='closed' AND closed_ts>=?", (m, day_start()))
    return dict(open_n=int(open_n), exposure=exposure, loss_all=max(0.0, -pnl_all), loss_day=max(0.0, -pnl_day),
                pnl_all=pnl_all, pnl_day=pnl_day)


def blocked_reason(con, s, leader_buy_ts):
    if kill_on(s):
        return "kill switch is on"
    if time.time() - _state["control_ok"] > CONTROL_STALE_S:
        return f"couldn't read {CONTROL_FILE} (the kill switch) in the last 10 minutes"
    if _state["wallet_too_big"]:
        return "wallet holds more than LIVE_MAX_WALLET_USD"
    b = book(con, s["mode"])
    if b["open_n"] >= s["LIVE_MAX_OPEN"]:
        return f"{b['open_n']} live trades already open"
    if b["loss_day"] >= s["LIVE_DAILY_LOSS_USD"]:
        return f"daily loss limit hit (${b['loss_day']:.2f} lost today)"
    if b["exposure"] + b["loss_all"] + s["LIVE_TRADE_USD"] > s["LIVE_TOTAL_BUDGET_USD"] + 1e-9:
        return (f"budget used up (${b['exposure']:.2f} open + ${b['loss_all']:.2f} lost, "
                f"budget ${s['LIVE_TOTAL_BUDGET_USD']:.2f})")
    if leader_buy_ts and time.time() - leader_buy_ts > s["LIVE_MAX_SIGNAL_AGE_S"]:
        return f"signal too old ({time.time() - leader_buy_ts:.0f}s after the smart wallet bought)"
    return None


def check_wallet(con, s):
    """SOL balance, and the max-wallet-size rail. Returns the SOL balance, or None when no wallet is set (dry-run)."""
    if not s["address"]:
        return None
    bal = sol_balance(s)
    usd = bal * sol_usd(s)
    too_big = usd > s["LIVE_MAX_WALLET_USD"]
    if too_big and not _state["wallet_too_big"]:
        event(con, "blocked", f"wallet holds about ${usd:.2f}, more than LIVE_MAX_WALLET_USD "
                              f"(${s['LIVE_MAX_WALLET_USD']:.2f}); no live trades until it's lower")
    _state["wallet_too_big"] = too_big
    return bal


# ----------------------------------------------------------------------------- hooks called by scanner.py
def on_paper_buy(con, signal_id, mint, symbol, leader_buy_ts):
    """The paper account just copied a buy. Live / dry-run buys the same coin if every rail allows it."""
    if mode() == "off":
        return
    s = ready(con)
    if not s:
        return
    name = symbol or mint[:6]
    try:
        bal = check_wallet(con, s)
        why = blocked_reason(con, s, leader_buy_ts)
        if why:
            event(con, "skip", f"{name}: not bought, {why}"); con.commit(); return
        px = sol_usd(s)
        lamports = int(s["LIVE_TRADE_USD"] / px * LAMPORTS)
        if bal is not None and bal - lamports / LAMPORTS < FEE_RESERVE_SOL:
            event(con, "skip", f"{name}: not bought, wallet has {bal:.4f} SOL, too little after the fee reserve")
            con.commit(); return
        q = quote(s, SOL, mint, lamports)
        imp = impact_pct(q)
        if imp > s["LIVE_MAX_PRICE_IMPACT_PCT"]:
            event(con, "skip", f"{name}: not bought, price impact {imp:.2f}% is over the limit")
            con.commit(); return
        if s["mode"] == "dry-run":
            con.execute("""INSERT INTO live_trades(mode,signal_id,mint,symbol,opened_ts,sol_in,usd_in,token_raw,
                price_impact_pct,status,notes) VALUES('dry-run',?,?,?,?,?,?,?,?,'open',?)""",
                        (signal_id, mint, symbol, time.time(), lamports / LAMPORTS, s["LIVE_TRADE_USD"], q["outAmount"],
                         imp, "quote only, nothing signed or sent"))
            event(con, "dry-buy", f"{name}: would buy ${s['LIVE_TRADE_USD']:.2f} ({lamports / LAMPORTS:.4f} SOL), "
                                  f"quote {q['outAmount']} raw tokens, impact {imp:.2f}%")
        else:
            before = token_balances(s, mint).get(mint, 0)
            sol_before = bal
            sig = _swap_and_send(s, q)
            got = token_balances(s, mint).get(mint, 0) - before
            spent = max(0.0, sol_before - sol_balance(s))
            con.execute("""INSERT INTO live_trades(mode,signal_id,mint,symbol,opened_ts,buy_sig,sol_in,usd_in,token_raw,
                price_impact_pct,status) VALUES('live',?,?,?,?,?,?,?,?,?,'open')""",
                        (signal_id, mint, symbol, time.time(), sig, spent, spent * px, str(got), imp))
            event(con, "buy", f"{name}: bought {got} raw tokens for {spent:.4f} SOL (${spent * px:.2f}), tx {sig}")
        con.commit()
    except Exception as e:
        event(con, "error", f"{name}: buy failed, {e}"); con.commit()


def _sell(con, s, t, reason):
    name = t["symbol"] or t["mint"][:6]
    px = sol_usd(s)
    if s["mode"] == "dry-run":
        amt = int(t["token_raw"] or 0)
        sol_out = int(quote(s, t["mint"], SOL, amt)["outAmount"]) / LAMPORTS if amt else 0.0
        usd_out = sol_out * px - 2 * C.NETWORK_FEE_USD   # rough network fees for the buy and the sell
        sig = None
    else:
        amt = token_balances(s, t["mint"]).get(t["mint"], 0)
        if amt == 0:
            sol_out, usd_out, sig = 0.0, 0.0, None
        else:
            sol_before = sol_balance(s)
            sig = _swap_and_send(s, quote(s, t["mint"], SOL, amt))
            sol_out = max(0.0, sol_balance(s) - sol_before)
            usd_out = sol_out * px
    pnl = usd_out - (t["usd_in"] or 0)
    con.execute("""UPDATE live_trades SET status='closed',closed_ts=?,sell_sig=?,sol_out=?,usd_out=?,pnl_usd=?,close_reason=?
                   WHERE id=?""", (time.time(), sig, sol_out, usd_out, pnl, reason, t["id"]))
    event(con, "dry-sell" if s["mode"] == "dry-run" else "sell",
          f"{name}: sold ({reason}) for ${usd_out:.2f}, P&L ${pnl:+.2f}" + (f", tx {sig}" if sig else ""))


def on_paper_close(con, signal_id, mint, reason):
    """The paper account just closed a trade. Live / dry-run sells the same coin."""
    if mode() == "off":
        return
    s = ready(con)
    if not s:
        return
    for t in con.execute("SELECT * FROM live_trades WHERE mode=? AND signal_id=? AND mint=? AND status IN ('open','sell_failed')",
                         (s["mode"], signal_id, mint)).fetchall():
        try:
            _sell(con, s, t, reason)
        except Exception as e:
            con.execute("UPDATE live_trades SET status='sell_failed', close_reason=?, sell_tries=sell_tries+1 WHERE id=?",
                        (reason, t["id"]))
            event(con, "error", f"{t['symbol'] or mint[:6]}: sell failed, will retry, {e}")
        con.commit()


_last_tick = {"t": 0.0, "n": 0}
DRY_GIVE_UP_TRIES = 10


def tick(con):
    """Once a minute: re-read the kill switch, retry failed sells (backing off after 10 tries), run the kill switch."""
    if mode() == "off":
        return
    s = ready(con)
    if not s or time.time() - _last_tick["t"] < 60:
        return
    _last_tick["t"] = time.time(); _last_tick["n"] += 1
    kill = kill_on(s)
    rows = con.execute("SELECT * FROM live_trades WHERE mode=? AND status IN ('open','sell_failed')", (s["mode"],)).fetchall()
    for t in rows:
        if not (t["status"] == "sell_failed" or kill):
            continue
        if not kill and t["sell_tries"] >= 10 and _last_tick["n"] % 15:
            continue   # after 10 failed tries, retry every 15 minutes
        try:
            _sell(con, s, t, "kill switch" if kill else (t["close_reason"] or "retry"))
        except Exception as e:
            con.execute("UPDATE live_trades SET status='sell_failed', sell_tries=sell_tries+1 WHERE id=?", (t["id"],))
            if s["mode"] == "dry-run" and t["sell_tries"] + 1 >= DRY_GIVE_UP_TRIES:
                con.execute("""UPDATE live_trades SET status='closed',closed_ts=?,sol_out=0,usd_out=0,pnl_usd=-usd_in,
                               close_reason=? WHERE id=?""", (time.time(), "no route to sell, counted as a total loss", t["id"]))
            event(con, "error", f"{t['symbol'] or t['mint'][:6]}: sell retry failed, {e}")
        con.commit()
    if kill and s["mode"] == "live" and not _state.get("swept"):
        # the kill switch also sells any other coin left in the wallet (it should only ever hold what the bot bought)
        event(con, "kill", "kill switch is on: no new trades, selling every coin in the wallet")
        try:
            left = {m: a for m, a in token_balances(s).items() if m not in C.EXCLUDE_MINTS}
            for m, a in left.items():
                try:
                    _swap_and_send(s, quote(s, m, SOL, a))
                    event(con, "kill", f"sold leftover coin {m[:6]}")
                except Exception as e:
                    event(con, "error", f"kill switch could not sell {m[:6]}: {e}")
            _state["swept"] = True
        except Exception as e:
            event(con, "error", f"kill switch could not read the wallet: {e}")
        con.commit()


# ----------------------------------------------------------------------------- dashboard
def dashboard_status(con):
    m = mode()
    if m == "off":
        return {"mode": "off"}
    out = {"mode": m}
    try:
        if con.execute("SELECT name FROM sqlite_master WHERE name='live_trades'").fetchone():
            book_mode = "live" if m == "live" else "dry-run"
            out["book"] = book(con, book_mode)
            out["events"] = [dict(ts=r[0], kind=r[1], msg=r[2]) for r in
                             con.execute("SELECT ts,kind,msg FROM live_events ORDER BY ts DESC LIMIT 30").fetchall()]
    except Exception:
        pass
    return out


# ----------------------------------------------------------------------------- command line
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "check":
        s, problems = load_settings()
        print("LIVE_MODE:", mode())
        if mode() == "off":
            print("Live mode is off. Paper mode only.")
        elif problems:
            print("Live mode would REFUSE to start:"); [print(" -", p) for p in problems]
        else:
            print("Settings are complete.", {k: v for k, v in s.items() if k not in ("jup_key", "rpc")})
