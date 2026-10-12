"""Record Hyperliquid public trades and best bid/offer for a set of markets.

Markets: coins.txt plus every listed HIP-3 perp (refreshed at start). Writes
rec/YYYYMMDD-HH.jsonl.gz (one line per message: {"r": recv_ms, "ch": "trades"|"bbo", "d": data}).

  python record.py SECONDS [HANDOFF_SECONDS] [NEXT_SECONDS,NEXT_HANDOFF]

Stops after SECONDS. If HANDOFF_SECONDS > 0, runs `gh workflow run record.yml` at that point
so the next run starts before this one ends (overlap; duplicates are harmless).
"""
import asyncio
import gzip
import json
import subprocess
import sys
import time
from pathlib import Path

import requests
import websockets

OUT = Path(__file__).parent / "rec"
API = "https://api.hyperliquid.xyz/info"


def markets():
    coins = set(Path(__file__).with_name("coins.txt").read_text().split())
    for d in requests.post(API, json={"type": "perpDexs"}, timeout=30).json()[1:]:
        m, _ = requests.post(API, json={"type": "metaAndAssetCtxs", "dex": d["name"]}, timeout=30).json()
        coins |= {u["name"] for u in m["universe"] if not u.get("isDelisted")}
    return sorted(coins)


async def run(coins, stop_at, handoff_at):
    OUT.mkdir(exist_ok=True)
    handed = handoff_at is None
    fh, cur = None, None
    while time.time() < stop_at:
        try:
            async with websockets.connect("wss://api.hyperliquid.xyz/ws", ping_interval=None, max_size=None) as ws:
                for c in coins:
                    for t in ("trades", "bbo"):
                        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": t, "coin": c}}))
                last_ping = time.time()
                while time.time() < stop_at:
                    if not handed and time.time() >= handoff_at:
                        handed = True
                        ns, nh = NEXT.split(",")
                        r = subprocess.run(["gh", "workflow", "run", "record.yml", "-f", f"seconds={ns}", "-f", f"handoff={nh}"],
                                           capture_output=True, text=True)
                        print("handoff:", r.returncode, r.stderr.strip()[:200], flush=True)
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=20)
                    except asyncio.TimeoutError:
                        msg = None
                    if time.time() - last_ping > 30:
                        await ws.send(json.dumps({"method": "ping"}))
                        last_ping = time.time()
                    if msg is None:
                        continue
                    m = json.loads(msg)
                    if m.get("channel") not in ("trades", "bbo"):
                        continue
                    name = time.strftime("%Y%m%d-%H.jsonl.gz", time.gmtime())
                    if name != cur:
                        if fh:
                            fh.close()
                        fh, cur = gzip.open(OUT / name, "at"), name
                    fh.write(json.dumps({"r": int(time.time() * 1000), "ch": m["channel"], "d": m["data"]}) + "\n")
        except Exception as e:  # noqa: BLE001
            print(time.strftime("%H:%M:%S"), "reconnect after", repr(e)[:120], flush=True)
            await asyncio.sleep(5)
    if fh:
        fh.close()


if __name__ == "__main__":
    secs = float(sys.argv[1])
    handoff = float(sys.argv[2]) if len(sys.argv) > 2 and float(sys.argv[2]) > 0 else None
    NEXT = sys.argv[3] if len(sys.argv) > 3 else "21000,20400"
    cs = markets()
    print(len(cs), "markets", flush=True)
    t0 = time.time()
    asyncio.run(run(cs, t0 + secs, t0 + handoff if handoff else None))
