#!/usr/bin/env python3
"""Runs the PAPER-MODE scanner inside a GitHub Actions job and publishes the dashboard data.

PAPER MODE ONLY: no wallet, no keys, nothing is ever signed or sent on-chain.

Usage:
  python ci_runner.py prepare            # make sure state/scanner.db exists (from cache, else from the encrypted seed)
  python ci_runner.py run MINUTES [last] # run the scanner for MINUTES, publishing dashboard data every 2 minutes
  python ci_runner.py pack               # fold the database journal in and trim the log before the cache is saved

The dashboard data is encrypted (AES-256-GCM) with DASH_KEY before it leaves the runner, because the repository
is public. Only someone holding the key (it travels in the dashboard link after the #) can read it.
"""
import base64, gzip, json, os, signal, sqlite3, subprocess, sys, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, "state")
DB = os.path.join(STATE, "scanner.db")
LOG = os.path.join(STATE, "scanner.log")
os.environ["SCANNER_DB"] = DB
os.environ["SCANNER_LOG"] = LOG
os.environ["QUIET"] = "1"
PUB = os.path.join(HERE, "_publish")
RSTATE = os.path.join(HERE, "radium_state")
RADIUM_PID = os.path.join(RSTATE, "daemon.pid")
# Rage Radium Bot's wallet key: taken out of this process's environment at once, so the paper scanner (started
# from here) never has it. It is handed only to the Radium process, and only its environment.
_RADIUM_KEY = os.environ.pop("RADIUM_WALLET_KEY", "")
RADIUM_ON = (os.environ.get("RADIUM_MODE", "").strip().lower() or "dry-run") != "off"
REPO = os.environ.get("GITHUB_REPOSITORY", "")
TOKEN = os.environ.get("GITHUB_TOKEN", "")
PUBLISH_EVERY = 60   # dashboard data every minute (was 2 min; Nov 8)


def key():
    k = os.environ["DASH_KEY"].strip()
    return base64.urlsafe_b64decode(k + "=" * (-len(k) % 4))


def aes():
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    return AESGCM(key())


def encrypt(b):
    n = os.urandom(12)
    return base64.b64encode(n + aes().encrypt(n, b, None))


def decrypt(b):
    raw = base64.b64decode(b)
    return aes().decrypt(raw[:12], raw[12:], None)


def prepare():
    os.makedirs(STATE, exist_ok=True)
    if os.path.exists(DB) and os.path.getsize(DB) > 0:
        print("state restored from cache")
        return
    seed = os.path.join(HERE, "seed", "scanner.db.gz.enc")
    with open(seed, "rb") as f:
        data = gzip.decompress(decrypt(f.read()))
    with open(DB, "wb") as f:
        f.write(data)
    print("state started from seed")


def git(*a):
    return subprocess.run(["git", *a], cwd=PUB, capture_output=True, text=True)


def radium_status():
    try:
        with open(os.path.join(RSTATE, "status.json")) as f:
            return json.load(f)
    except Exception:
        return None


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
    """Starts Rage Radium Bot in the background. It keeps running across the hourly steps of this job (the
    paper scanner restarts each hour, Radium doesn't), and hands its lock to the bridge job when the run ends."""
    if not RADIUM_ON or radium_alive() or os.path.exists(os.path.join(RSTATE, "release")):
        return
    os.makedirs(RSTATE, exist_ok=True)
    env = dict(os.environ)
    if _RADIUM_KEY:
        env["RADIUM_WALLET_KEY"] = _RADIUM_KEY
    subprocess.Popen([sys.executable, os.path.join(HERE, "radium.py"), "--role", "primary"], cwd=HERE, env=env,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    print("Rage Radium Bot started")


def radium_release(wait_s=150):
    """End of the run: tell Radium to hand its lock (and state) to the bridge, and wait until it has."""
    os.makedirs(RSTATE, exist_ok=True)
    if radium_alive() and not os.path.exists(os.path.join(RSTATE, "bridge_sent")):
        dispatch_bridge()     # run cut short (cancelled or failed): still get a bridge to take over open trades
        time.sleep(45)
    open(os.path.join(RSTATE, "release"), "w").close()
    t0 = time.time()
    while radium_alive() and time.time() - t0 < wait_s:
        time.sleep(1)
    print("Rage Radium Bot handed over" if not radium_alive() else "Rage Radium Bot did not exit in time")


def dispatch_bridge():
    req = urllib.request.Request(
        f"https://api.github.com/repos/{REPO}/actions/workflows/radium-bridge.yml/dispatches",
        data=json.dumps({"ref": "main"}).encode(), method="POST",
        headers={"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "paper-scanner"})
    try:
        urllib.request.urlopen(req, timeout=20)
        open(os.path.join(RSTATE, "bridge_sent"), "w").close()
        print("Radium bridge queued")
    except Exception as e:
        print("could not queue the Radium bridge:", type(e).__name__)


def publish(extra):
    sys.path.insert(0, HERE)
    import dashboard
    st = dashboard.build_state()
    st.update(extra)
    rs = radium_status()
    if rs is not None:
        st["radium"] = rs
    blob = encrypt(gzip.compress(json.dumps(st, default=str, separators=(",", ":")).encode()))
    if not os.path.isdir(os.path.join(PUB, ".git")):
        os.makedirs(PUB, exist_ok=True)
        git("init", "-q", "-b", "data")
        git("config", "user.name", "paper-scanner")
        git("config", "user.email", "paper-scanner@users.noreply.github.com")
        git("remote", "add", "origin", f"https://x-access-token:{TOKEN}@github.com/{REPO}.git")
    with open(os.path.join(PUB, "state.enc"), "wb") as f:
        f.write(blob)
    git("add", "state.enc")
    first = git("rev-parse", "-q", "--verify", "HEAD").returncode != 0
    git("commit", "-q", "-m", "dashboard data", *([] if first else ["--amend"]))
    r = git("push", "-q", "-f", "origin", "HEAD:data")
    if r.returncode != 0:
        print("publish failed")


def dispatch_next():
    """Queue the next run so the scanner carries on as soon as this one ends."""
    req = urllib.request.Request(
        f"https://api.github.com/repos/{REPO}/actions/workflows/scan.yml/dispatches",
        data=json.dumps({"ref": "main"}).encode(), method="POST",
        headers={"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "paper-scanner"})
    try:
        urllib.request.urlopen(req, timeout=20)
        print("next run queued")
    except Exception as e:
        print("could not queue next run:", type(e).__name__)


def run(minutes, last):
    end = time.time() + minutes * 60
    run_url = f"https://github.com/{REPO}/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}"
    extra = {"host": "github", "repo": REPO, "run_url": run_url, "segment_ends": end, "run_last_segment": last}
    proc = subprocess.Popen([sys.executable, os.path.join(HERE, "scanner.py")], cwd=HERE,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    queued = False
    bridged = False
    next_pub = time.time() + 45
    radium_start()
    while time.time() < end:
        if proc.poll() is not None:  # crashed: start it again
            time.sleep(5)
            proc = subprocess.Popen([sys.executable, os.path.join(HERE, "scanner.py")], cwd=HERE,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        radium_start()   # no-op while it runs; restarts it if it stopped
        if last and RADIUM_ON and not bridged and end - time.time() < 420:
            dispatch_bridge(); bridged = True
        if last and not queued and end - time.time() < 240:
            dispatch_next(); queued = True
        if time.time() >= next_pub:
            try:
                publish(extra)
            except Exception as e:
                print("publish error:", type(e).__name__)
            next_pub = time.time() + PUBLISH_EVERY
        time.sleep(5)
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(30)
    except subprocess.TimeoutExpired:
        proc.kill()
    if last and RADIUM_ON:
        radium_release()
    pack()
    try:
        publish(extra)
    except Exception as e:
        print("publish error:", type(e).__name__)


def pack():
    try:
        c = sqlite3.connect(DB)
        c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        c.close()
    except Exception as e:
        print("checkpoint error:", type(e).__name__)
    if os.path.exists(LOG) and os.path.getsize(LOG) > 2_000_000:
        with open(LOG, "rb") as f:
            f.seek(-1_500_000, 2)
            tail = f.read()
        with open(LOG, "wb") as f:
            f.write(tail[tail.find(b"\n") + 1:])
    for old in (LOG + ".1", LOG + ".2", LOG + ".3"):
        if os.path.exists(old):
            os.remove(old)


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "prepare":
        prepare()
    elif cmd == "run":
        run(float(sys.argv[2]), len(sys.argv) > 3 and sys.argv[3] == "last")
    elif cmd == "pack":
        pack()
    elif cmd == "radium-release":
        radium_release()
    elif cmd == "seal":  # used once, on setup: encrypt a database file as the seed
        with open(sys.argv[2], "rb") as f:
            sys.stdout.buffer.write(encrypt(gzip.compress(f.read(), 6)))
