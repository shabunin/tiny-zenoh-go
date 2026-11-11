"""Reference Zenoh peer for gozenoh cross-implementation tests.

Drives the official eclipse-zenoh Python binding (the Rust Zenoh stack) so Go
tests can check that gozenoh interoperates with it in both directions.
Prints one JSON object per event on stdout; prints {"ready": true} once its
entities are declared.

usage: zpeer.py <router | json-config> <mode> <args...>
  <router> is a locator to connect to in client mode; a JSON object instead is
  a set of config inserts, e.g. {"mode": "peer", "listen/endpoints": [...]}.

  sub <key> <n>             print n samples
  pub <key>                 put (text/plain;x, attachment) then delete
  queryable <key> <secs>    reply "py:<params>:<payload>" on the query key; params "fail" -> error
  get <selector>            print replies (sends payload "qbody")
  publisher <key> <n>       declared publisher (writer-side filtered): n puts "pub-<i>"
  querier <key>             declared querier: one get, print replies
  matching <pub|querier|querier-complete> <key> <secs>
                            print {"matching": bool} on each matching status change
  advpub <key> <n> <secs>   advanced publisher (cache 10, miss detection with
                            0.3s heartbeat, publisher detection): n puts "adv-<i>", ready, wait
  advsub <key> <n> <hist|hb>  advanced subscriber (hist: history of late publishers,
                            max 10, recovery with 1s periodic queries; hb: heartbeat recovery),
                            print n samples and {"miss": n} events
  token <key> <secs>        hold a liveliness token
  livsub <key> <n>          liveliness subscriber with history, print n events
  bigsub <key> <n>          print size/sha256/shm of n samples
  bigpub <key> <size> <n>   put n pattern payloads of size bytes
  bigqueryable <key> <size> <secs>  reply a pattern payload; print the query body digest
  bigget <selector> <size>  get with a pattern body; print reply digests
"""
import hashlib
import json
import sys
import threading
import time

import zenoh


def emit(**kw):
    print(json.dumps(kw), flush=True)


def sample_dict(s):
    return {
        "key": str(s.key_expr),
        "kind": "DELETE" if s.kind == zenoh.SampleKind.DELETE else "PUT",
        "payload": s.payload.to_bytes().decode("latin-1"),
        "encoding": str(s.encoding),
        "attachment": s.attachment.to_bytes().decode("latin-1") if s.attachment is not None else None,
        "priority": int(s.priority),
    }


def pattern(n):
    return bytes((i * 7 + 3) % 251 for i in range(n))


def digest(zb):
    b = zb.to_bytes()
    return {"size": len(b), "sum": hashlib.sha256(b).hexdigest(), "shm": zb.as_shm() is not None}


def main():
    target, mode, args = sys.argv[1], sys.argv[2], sys.argv[3:]
    zenoh.try_init_log_from_env()  # RUST_LOG=... to debug
    conf = zenoh.Config()
    if target.startswith("{"):
        for k, v in json.loads(target).items():
            conf.insert_json5(k, json.dumps(v))
    else:
        conf.insert_json5("mode", '"client"')
        conf.insert_json5("connect/endpoints", json.dumps([target]))
        conf.insert_json5("scouting/multicast/enabled", "false")
    s = zenoh.open(conf)
    done = threading.Event()

    if mode in ("sub", "livsub"):
        key, n = args[0], int(args[1])
        count = [0]

        def on(smp):
            emit(**sample_dict(smp))
            count[0] += 1
            if count[0] >= n:
                done.set()

        if mode == "sub":
            h = s.declare_subscriber(key, on)
        else:
            h = s.liveliness().declare_subscriber(key, on, history=True)
        emit(ready=True)
        done.wait(10)
        h.undeclare()
    elif mode == "pub":
        key = args[0]
        s.put(key, b"from-rust", encoding="text/plain;x", attachment=b"att")
        s.delete(key)
        time.sleep(0.3)
    elif mode == "puts":
        for key in args:
            s.put(key, key.encode())
        time.sleep(0.3)
    elif mode == "prio":
        key = args[0]
        for name in args[1:]:
            p = getattr(zenoh.Priority, name)
            s.put(key, str(int(p)).encode(), priority=p)
        time.sleep(0.3)
    elif mode == "queryable":
        key, secs = args[0], float(args[1])

        def on_query(q):
            params = str(q.parameters)
            if "fail" in params:
                q.reply_err(b"rust-err")
                return
            body = q.payload.to_bytes().decode() if q.payload is not None else ""
            q.reply(q.key_expr, f"py:{params}:{body}".encode())

        h = s.declare_queryable(key, on_query)
        emit(ready=True)
        time.sleep(secs)
        h.undeclare()
    elif mode == "get":
        for r in s.get(args[0], payload=b"qbody", timeout=5.0):
            if r.ok is not None:
                emit(ok=sample_dict(r.ok))
            else:
                emit(err=r.err.payload.to_bytes().decode("latin-1"))
        emit(done=True)
    elif mode == "publisher":
        key, n = args[0], int(args[1])
        pub = s.declare_publisher(key)
        time.sleep(0.5)  # let the router answer the publisher's interest
        for i in range(n):
            pub.put(f"pub-{i}".encode())
        time.sleep(0.3)
        emit(done=True)
    elif mode == "querier":
        qr = s.declare_querier(args[0], timeout=5.0)
        time.sleep(0.5)
        for r in qr.get(payload=b"qbody"):
            if r.ok is not None:
                emit(ok=sample_dict(r.ok))
            else:
                emit(err=r.err.payload.to_bytes().decode("latin-1"))
        emit(done=True)
    elif mode == "bigsub":
        key, n = args[0], int(args[1])
        count = [0]

        def on(smp):
            emit(key=str(smp.key_expr), **digest(smp.payload))
            count[0] += 1
            if count[0] >= n:
                done.set()

        h = s.declare_subscriber(key, on)
        emit(ready=True)
        done.wait(10)
        h.undeclare()
    elif mode == "bigpub":
        key, size, n = args[0], int(args[1]), int(args[2])
        pub = s.declare_publisher(key)
        time.sleep(0.5)
        for i in range(n):
            pub.put(pattern(size))
            time.sleep(0.05)
        time.sleep(0.3)
        emit(done=True)
    elif mode == "bigqueryable":
        key, size, secs = args[0], int(args[1]), float(args[2])

        def on_query(q):
            if q.payload is not None:
                emit(**digest(q.payload))
            q.reply(q.key_expr, pattern(size))

        h = s.declare_queryable(key, on_query)
        emit(ready=True)
        time.sleep(secs)
        h.undeclare()
    elif mode == "bigget":
        sel, size = args[0], int(args[1])
        time.sleep(0.5)
        for r in s.get(sel, payload=pattern(size), timeout=5.0):
            if r.ok is not None:
                emit(**digest(r.ok.payload))
        emit(done=True)
    elif mode == "matching":
        kind, key, secs = args[0], args[1], float(args[2])
        if kind == "pub":
            ent = s.declare_publisher(key)
        elif kind == "querier":
            ent = s.declare_querier(key)
        else:
            ent = s.declare_querier(key, target=zenoh.QueryTarget.ALL_COMPLETE)
        lst = ent.declare_matching_listener(lambda st: emit(matching=st.matching))
        emit(ready=True)
        time.sleep(secs)
        lst.undeclare()
        ent.undeclare()
    elif mode == "advpub":
        import zenoh.ext as zx
        key, n, secs = args[0], int(args[1]), float(args[2])
        pub = zx.declare_advanced_publisher(
            s, key, cache=zx.CacheConfig(10),
            sample_miss_detection=zx.MissDetectionConfig(heartbeat=0.3, sporadic_heartbeat=None),
            publisher_detection=True)
        for i in range(n):
            pub.put(f"adv-{i}".encode())
        emit(ready=True)
        time.sleep(secs)
        pub.undeclare()
    elif mode == "advsub":
        import zenoh.ext as zx
        key, n, kind = args[0], int(args[1]), args[2]
        count = [0]

        def on_sample(smp):
            emit(**sample_dict(smp))
            count[0] += 1
            if count[0] >= n:
                done.set()

        history = zx.HistoryConfig(detect_late_publishers=True, max_samples=10) if kind == "hist" else None
        if kind == "hb":
            recovery = zx.RecoveryConfig(periodic_queries=None, heartbeat=True)
        else:  # the binding wants one of the two; gaps trigger queries anyway
            import datetime
            recovery = zx.RecoveryConfig(periodic_queries=datetime.timedelta(seconds=1), heartbeat=None)
        sub = zx.declare_advanced_subscriber(s, key, on_sample, history=history, recovery=recovery)
        ml = sub.sample_miss_listener(lambda m: emit(miss=m.nb))
        emit(ready=True)
        done.wait(30)
        ml.undeclare()
        sub.undeclare()
    elif mode == "token":
        key, secs = args[0], float(args[1])
        t = s.liveliness().declare_token(key)
        emit(ready=True)
        time.sleep(secs)
        t.undeclare()
    else:
        sys.exit("unknown mode " + mode)
    s.close()


if __name__ == "__main__":
    main()
