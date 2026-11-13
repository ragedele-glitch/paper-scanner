"""Runs Rage Radium Bot as its own GitHub Actions job (radium.yml), separate from the paper scanner (scan.yml).

Two processes:
  - its own signal engine: scanner.py with RADIUM_ENGINE=1 and its own database (engine_state/scanner.db). It
    follows the same wallets as the paper main account, runs the main account's filters and exits, and opens one $20
    reference position per signal that passes. It restarts each hour so its database is saved between steps.
  - radium.py (the trader): buys when the engine opens, sells when the engine sells, and guards open trades itself
    while the engine restarts. It keeps running across the hourly steps and hands its lock to the bridge job at the
    end of the run, exactly as before.
Nothing here signs anything. Live trading needs RADIUM_MODE=live, the key secret, and Rage's "go".

Wallet list: the engine follows the paper main account's current wallets. At the start of a run they come from the
paper scanner's saved state (restored read-only into state/); while running, from follow.enc on the data branch,
which the paper job publishes every 2 minutes. Removed wallets are always left out (config.REMOVED_WALLETS).
"""
import base64, gzip, json, os, signal, sqlite3, subprocess, sys, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ESTATE = os.path.join(HERE, "engine_state")
EDB = os.path.join(ESTATE, "scanner.db")
ELOG = os.path.join(ESTATE, "scanner.log")
FOLLOW = os.path.join(ESTATE, "follow.json")
PAPER_WALLETS = os.path.join(ESTATE, "paper_wallets.json")   # main bot's wallet ranking and scorecard, for the page only
PAPER_DB = os.path.join(HERE, "state", "scanner.db")     # paper scanner's saved state, read only, for the wallet list
RSTATE = os.environ.get("RADIUM_STATE_DIR", os.path.join(HERE, "radium_state"))
RADIUM_PID = os.path.join(RSTATE, "daemon.pid")
REPO = os.environ.get("GITHUB_REPOSITORY", "")
TOKEN = os.environ.get("GITHUB_TOKEN", "")
# The wallet key only goes to radium.py, never to the engine.
_RADIUM_KEY = os.environ.pop("RADIUM_WALLET_KEY", "")
RADIUM_ON = (os.environ.get("RADIUM_MODE", "").strip().lower() or "dry-run") != "off"


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def aes():
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    k = os.environ["DASH_KEY"].strip()
    return AESGCM(base64.urlsafe_b64decode(k + "=" * (-len(k) % 4)))


def write_follow(wallets, src):
    wallets = [w for w in wallets if w.get("address")]
    if not wallets:
        return False
    new = json.dumps({"wallets": wallets, "src": src}, indent=1)
    try:
        with open(FOLLOW) as f:
            old = json.load(f)
        if old.get("wallets") == wallets:
            return False
    except Exception:
        pass
    tmp = FOLLOW + ".tmp"
    with open(tmp, "w") as f:
        f.write(new)
    os.replace(tmp, FOLLOW)
    log(f"wallet list: {len(wallets)} wallets from {src}")
    return True


def follow_from_paper_db():
    if not os.path.exists(PAPER_DB):
        return False
    try:
        c = sqlite3.connect(f"file:{PAPER_DB}?mode=ro", uri=True, timeout=10)
        rows = c.execute("SELECT address, name FROM wallets WHERE active=1 ORDER BY last_rank").fetchall()
        c.close()
    except Exception as e:
        log("paper wallet list not read:", type(e).__name__)
        return False
    return write_follow([{"address": a, "name": n or ""} for a, n in rows], "paper saved state")


def follow_from_data_branch():
    """follow.enc on the data branch: the paper main account's current wallet list, sealed with DASH_KEY."""
    if not REPO:
        return False
    try:
        req = urllib.request.Request(f"https://api.github.com/repos/{REPO}/contents/follow.enc?ref=data&t={int(time.time())}",
                                     headers={"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github.raw",
                                              "User-Agent": "rage-radium-bot"})
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = base64.b64decode(r.read().strip())
        d = json.loads(gzip.decompress(aes().decrypt(raw[:12], raw[12:], None)))
    except Exception as e:
        if "404" not in str(e):
            log("follow.enc not read:", type(e).__name__)
        return False
    if time.time() - (d.get("ts") or 0) > 3 * 3600:
        return False     # stale: the paper job isn't publishing, keep what we have
    return write_follow(d.get("wallets") or [], "paper live list")


def wallets_view_from_data_branch():
    """The main bot's wallet ranking (Kolscan rank, profit, wins/losses), its wallet scorecard and its per-wallet
    results, from state.enc on the data branch, for Radium's page only. Trading never reads this file."""
    if not REPO:
        return False
    try:
        req = urllib.request.Request(f"https://api.github.com/repos/{REPO}/contents/state.enc?ref=data&t={int(time.time())}",
                                     headers={"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github.raw",
                                              "User-Agent": "rage-radium-bot"})
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = base64.b64decode(r.read().strip())
        d = json.loads(gzip.decompress(aes().decrypt(raw[:12], raw[12:], None)))
        b = d.get("wallet_board") if isinstance(d.get("wallet_board"), dict) else None
        out = {"ts": d.get("now") or time.time(), "read": time.time(),
               "wallets": [{k: w.get(k) for k in ("name", "addr", "rank", "pnl_usd", "wins", "losses")}
                           for w in (d.get("wallets") or []) if isinstance(w, dict)],
               "card": d.get("wallet_card"),
               "board": ({k: b.get(k) for k in ("min_trades", "quick_dump_min", "real", "real_since", "error")} if b else None)}
        tmp = PAPER_WALLETS + ".tmp"
        with open(tmp, "w") as f:
            json.dump(out, f)
        os.replace(tmp, PAPER_WALLETS)
        return True
    except Exception as e:
        log("wallet ranking not read:", type(e).__name__)
        return False


def prepare():
    os.makedirs(ESTATE, exist_ok=True)
    os.makedirs(RSTATE, exist_ok=True)
    for f in ("release", "bridge_sent"):     # a new run starts fresh (these mark the end of the previous run)
        try:
            os.remove(os.path.join(RSTATE, f))
        except FileNotFoundError:
            pass
    log("engine state restored" if os.path.exists(EDB) else "engine starts with a fresh database")
    if not follow_from_data_branch():
        follow_from_paper_db()
    wallets_view_from_data_branch()
    if not os.path.exists(FOLLOW):
        log("WARNING: no wallet list yet: the engine follows nobody (no buys) until one arrives")


def engine_env():
    env = {k: v for k, v in os.environ.items() if not k.startswith("RADIUM_WALLET") and k not in ("LIVE_WALLET_KEY",)}
    env.update(RADIUM_ENGINE="1", SCANNER_DB=EDB, SCANNER_LOG=ELOG, RADIUM_FOLLOW_FILE=FOLLOW)
    return env


def start_engine():
    return subprocess.Popen([sys.executable, os.path.join(HERE, "scanner.py")], cwd=HERE, env=engine_env(),
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def radium_alive():
    try:
        with open(RADIUM_PID) as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return b"radium.py" in f.read()
    except Exception:
        return False


def radium_start():
    if not RADIUM_ON or radium_alive() or os.path.exists(os.path.join(RSTATE, "release")):
        return
    env = dict(os.environ, SCANNER_DB=EDB, RADIUM_SOURCE="engine")
    if _RADIUM_KEY:
        env["RADIUM_WALLET_KEY"] = _RADIUM_KEY
    subprocess.Popen([sys.executable, os.path.join(HERE, "radium.py"), "--role", "primary"], cwd=HERE, env=env,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    log("Rage Radium Bot started")
    time.sleep(3)   # let it write its pid file before the next check


def dispatch(workflow):
    req = urllib.request.Request(
        f"https://api.github.com/repos/{REPO}/actions/workflows/{workflow}/dispatches",
        data=json.dumps({"ref": "main"}).encode(), method="POST",
        headers={"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "rage-radium-bot"})
    try:
        urllib.request.urlopen(req, timeout=20)
        log(f"{workflow} queued")
        return True
    except Exception as e:
        log(f"could not queue {workflow}:", type(e).__name__)
        return False


def radium_release(wait_s=150):
    """End of the run: tell Radium to hand its lock (and state) to the bridge, and wait until it has."""
    os.makedirs(RSTATE, exist_ok=True)
    if radium_alive() and not os.path.exists(os.path.join(RSTATE, "bridge_sent")):
        if dispatch("radium-bridge.yml"):    # run cut short (cancelled or failed): still get a bridge for open trades
            open(os.path.join(RSTATE, "bridge_sent"), "w").close()
        time.sleep(45)
    open(os.path.join(RSTATE, "release"), "w").close()
    t0 = time.time()
    while radium_alive() and time.time() - t0 < wait_s:
        time.sleep(1)
    log("Rage Radium Bot handed over" if not radium_alive() else "Rage Radium Bot did not exit in time")


def pack():
    try:
        c = sqlite3.connect(EDB)
        c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        c.close()
    except Exception as e:
        log("checkpoint error:", type(e).__name__)
    if os.path.exists(ELOG) and os.path.getsize(ELOG) > 2_000_000:
        with open(ELOG, "rb") as f:
            f.seek(-1_500_000, 2)
            tail = f.read()
        with open(ELOG, "wb") as f:
            f.write(tail[tail.find(b"\n") + 1:])
    for old in (ELOG + ".1", ELOG + ".2", ELOG + ".3"):
        if os.path.exists(old):
            os.remove(old)


def run(minutes, last):
    end = time.time() + minutes * 60
    proc = start_engine()
    radium_start()
    bridged = queued = False
    next_follow = time.time() + 300
    while time.time() < end:
        if proc.poll() is not None:   # crashed: start it again (Radium guards open trades meanwhile)
            time.sleep(5)
            proc = start_engine()
        radium_start()
        if time.time() >= next_follow:
            follow_from_data_branch()
            wallets_view_from_data_branch()
            next_follow = time.time() + 300
        if last and RADIUM_ON and not bridged and end - time.time() < 420:
            if dispatch("radium-bridge.yml"):
                open(os.path.join(RSTATE, "bridge_sent"), "w").close()
            bridged = True
        if last and not queued and end - time.time() < 240:
            dispatch("radium.yml"); queued = True
        time.sleep(5)
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(30)
    except subprocess.TimeoutExpired:
        proc.kill()
    if last and RADIUM_ON:
        radium_release()
    pack()


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "prepare":
        prepare()
    elif cmd == "run":
        run(float(sys.argv[2]), len(sys.argv) > 3 and sys.argv[3] == "last")
    elif cmd == "radium-release":
        radium_release()
    elif cmd == "pack":
        pack()
