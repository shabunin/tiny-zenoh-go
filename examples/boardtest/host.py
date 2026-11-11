"""Host harness for the board tests: plays zenoh-rust's side of every step
run by examples/boardtest/agent on a device (through the official Python
binding, eclipse-zenoh ~= 1.10, with zenoh.ext).

    python host.py <router-locator> <prefix>... [--timeout secs]

e.g. python host.py tcp/127.0.0.1:7447 board/cardputer/plain board/stickc/serial

Each prefix is board/<name>/<profile>; the run for it starts when the device
says "start". Exit status 0 when every prefix passed all its steps.
"""

import datetime
import json
import queue
import sys
import threading
import time

import zenoh
import zenoh.ext as zx


class Fail(Exception):
    pass


class Run:
    def __init__(self, s, prefix):
        self.s, self.p = s, prefix
        self.inbox = queue.Queue()
        self.cmd = s.declare_publisher(prefix + "/host")
        self.result = None
        self.log = []

    def say(self, msg):
        self.cmd.put(msg)

    def note(self, msg):
        line = f"{time.strftime('%H:%M:%S')} {self.p}: {msg}"
        print(line, flush=True)

    def wait(self, want, timeout=30):
        """Waits for a status starting with want; device FAILs abort."""
        deadline = time.time() + timeout
        while True:
            left = deadline - time.time()
            if left <= 0:
                raise Fail(f"timeout waiting for {want!r}")
            try:
                msg = self.inbox.get(timeout=left)
            except queue.Empty:
                continue
            if msg.startswith("FAIL"):
                raise Fail("device: " + msg)
            if msg.startswith(want):
                return msg

    def run(self):
        try:
            self.steps()
            self.result = True
        except Fail as e:
            self.note("FAILED: " + str(e))
            self.say("FAIL " + str(e))
            self.result = False
        except Exception as e:  # harness bug: report it as a failure too
            self.note(f"harness error: {e!r}")
            self.say("FAIL harness error")
            self.result = False

    def steps(self):
        p, s = self.p, self.s
        self.say("start")

        # query: our queryable appears (device's querier matches), answers,
        # disappears (matching goes false).
        self.wait("query: ready")
        qbl = s.declare_queryable(
            p + "/q", lambda q: q.reply(q.key_expr, f"pong {q.parameters}"))
        self.wait("query: answered")
        qbl.undeclare()
        self.wait("PASS query")

        # match: the device publishes only once our subscriber matches.
        self.wait("match: ready")
        got = queue.Queue()
        sub = s.declare_subscriber(p + "/m", lambda smp: got.put(
            (smp.payload.to_string(), smp.attachment.to_string() if smp.attachment else "")))
        try:
            pay, att = got.get(timeout=10)
        except queue.Empty:
            raise Fail("match: no sample")
        self.say(f"match: got {pay} {att}")
        self.wait("PASS match")
        sub.undeclare()

        # advpub: late zenoh-ext subscriber gets the device's cache, then
        # live samples; a heartbeat-only subscriber then fetches all of it.
        self.wait("advpub: ready")
        hist, misses = queue.Queue(), []
        sub = zx.declare_advanced_subscriber(
            s, p + "/adv", lambda smp: hist.put(smp.payload.to_string()),
            history=zx.HistoryConfig(detect_late_publishers=True, max_samples=10),
            recovery=zx.RecoveryConfig(periodic_queries=datetime.timedelta(seconds=1), heartbeat=None))
        ml = sub.sample_miss_listener(lambda m: misses.append(m.nb))
        first = take(hist, 3, "advpub history")
        self.say("advpub: more")
        rest = take(hist, 2, "advpub live")
        ml.undeclare()
        sub.undeclare()
        want = [f"adv-{i}" for i in range(5)]
        if first + rest != want or misses:
            raise Fail(f"advpub: got {first + rest} misses {misses}")
        hb = queue.Queue()
        sub = zx.declare_advanced_subscriber(
            s, p + "/adv", lambda smp: hb.put(smp.payload.to_string()),
            recovery=zx.RecoveryConfig(periodic_queries=None, heartbeat=True))
        got = take(hb, 5, "advpub heartbeat recovery")
        sub.undeclare()
        if got != want:
            raise Fail(f"advpub heartbeat: got {got}")
        self.say("advpub: ok")
        self.wait("PASS advpub")

        # advsub: our zenoh-ext publisher's history reaches the device's
        # advanced subscriber, then live samples.
        self.wait("advsub: ready")
        pub = zx.declare_advanced_publisher(
            s, p + "/hist", cache=zx.CacheConfig(10),
            sample_miss_detection=zx.MissDetectionConfig(heartbeat=0.5, sporadic_heartbeat=None),
            publisher_detection=True)
        for i in range(3):
            pub.put(f"h-{i}")
        self.say("advsub: go")
        self.wait("advsub: got h-0,h-1,h-2")
        pub.put("h-3")
        pub.put("h-4")
        self.say("advsub: sent")
        self.wait("PASS advsub")
        pub.undeclare()

        # live: tokens both ways, and their disappearance.
        self.wait("live: ready")
        n = sum(1 for r in s.liveliness().get(p + "/alive", timeout=5.0) if r.ok)
        if n != 1:
            raise Fail(f"live: get found {n} tokens")
        gone = threading.Event()
        lsub = s.liveliness().declare_subscriber(
            p + "/alive", lambda smp: gone.set() if smp.kind == zenoh.SampleKind.DELETE else None)
        tok = s.liveliness().declare_token(p + "/host-alive")
        self.say("live: seen")
        self.wait("live: undeclared")
        if not gone.wait(10):
            raise Fail("live: device token never went away")
        lsub.undeclare()
        tok.undeclare()
        self.wait("PASS live")
        self.wait("DONE")


def take(q, n, what):
    out = []
    deadline = time.time() + 15
    while len(out) < n:
        try:
            out.append(q.get(timeout=max(0.01, deadline - time.time())))
        except queue.Empty:
            raise Fail(f"{what}: got {out}, want {n}")
    return out


def main():
    args = sys.argv[1:]
    timeout = 600.0
    if "--timeout" in args:
        i = args.index("--timeout")
        timeout = float(args[i + 1])
        del args[i:i + 2]
    router, prefixes = args[0], args[1:]
    c = zenoh.Config()
    c.insert_json5("mode", '"client"')
    c.insert_json5("connect/endpoints", json.dumps([router]))
    c.insert_json5("scouting/multicast/enabled", "false")
    s = zenoh.open(c)
    runs = {p: Run(s, p) for p in prefixes}
    threads = {}
    lock = threading.Lock()

    def on_status(smp):
        key = str(smp.key_expr)
        p = key[: -len("/status")]
        r = runs.get(p)
        if r is None:
            return
        msg = smp.payload.to_string()
        r.note(msg)
        if msg.startswith("start "):
            with lock:
                t = threads.get(p)
                if t is not None and t.is_alive():
                    return  # restarted mid-run: the old run fails on its own
                r.inbox = queue.Queue()
                t = threading.Thread(target=r.run, daemon=True)
                threads[p] = t
                t.start()
            return
        r.inbox.put(msg)

    sub = s.declare_subscriber("board/**/status", on_status)
    print("harness ready for", ", ".join(prefixes), flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline and any(r.result is None for r in runs.values()):
        time.sleep(0.2)
    sub.undeclare()
    ok = True
    for p, r in runs.items():
        res = {True: "PASS", False: "FAIL", None: "NOT RUN"}[r.result]
        print(f"{res} {p}", flush=True)
        ok = ok and r.result is True
    s.close()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
