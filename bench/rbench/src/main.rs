// Rust side of the tiny-zenoh-go benchmarks: zenoh's z_pub_thr, z_sub_thr,
// z_ping and z_pong with the same keys and options as the Go examples.
//
//   rbench pub_thr|sub_thr|ping|pong|router [-m mode] [-e ep]... [-l ep]... [-s size] [-n n] [-c count]
use std::time::{Duration, Instant};
use zenoh::qos::CongestionControl;
use zenoh::Wait;

struct Args {
    cmd: String,
    mode: String,
    connect: Vec<String>,
    listen: Vec<String>,
    size: usize,
    n: u64,
    count: u64,
    shm: bool,
}

fn args() -> Args {
    let mut a = Args { cmd: String::new(), mode: "peer".into(), connect: vec![], listen: vec![], size: 8, n: 100000, count: 10, shm: false };
    let v: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < v.len() {
        let next = |i: &mut usize| { *i += 1; v[*i].clone() };
        match v[i].as_str() {
            "-m" => a.mode = next(&mut i),
            "-e" => { let x = next(&mut i); a.connect.push(x) }
            "-l" => { let x = next(&mut i); a.listen.push(x) }
            "-s" => a.size = next(&mut i).parse().unwrap(),
            "-n" => a.n = next(&mut i).parse().unwrap(),
            "-c" => a.count = next(&mut i).parse().unwrap(),
            "--shm" => a.shm = true,
            c => a.cmd = c.to_string(),
        }
        i += 1;
    }
    a
}

fn open(a: &Args) -> zenoh::Session {
    let mut c = zenoh::Config::default();
    c.insert_json5("mode", &format!("{:?}", a.mode)).unwrap();
    c.insert_json5("scouting/multicast/enabled", "false").unwrap();
    if !a.connect.is_empty() {
        c.insert_json5("connect/endpoints", &format!("{:?}", a.connect)).unwrap();
    }
    if !a.listen.is_empty() {
        c.insert_json5("listen/endpoints", &format!("{:?}", a.listen)).unwrap();
    }
    c.insert_json5("transport/shared_memory/enabled", if a.shm { "true" } else { "false" }).unwrap();
    zenoh::open(c).wait().unwrap()
}

fn main() {
    let a = args();
    let s = open(&a);
    match a.cmd.as_str() {
        "pub_thr" => {
            let p = s.declare_publisher("test/thr").congestion_control(CongestionControl::Block).wait().unwrap();
            let data: zenoh::bytes::ZBytes = vec![7u8; a.size].into();
            loop {
                p.put(data.clone()).wait().unwrap();
            }
        }
        "sub_thr" => {
            let (n, count) = (a.n, a.count);
            let mut got = 0u64;
            let mut rounds = 0u64;
            let mut start = Instant::now();
            let mut total = 0f64;
            let (tx, rx) = std::sync::mpsc::channel();
            let _sub = s.declare_subscriber("test/thr").callback_mut(move |_| {
                if got == 0 { start = Instant::now(); }
                got += 1;
                if got == n {
                    let r = n as f64 / start.elapsed().as_secs_f64();
                    println!("{:.1} msg/s", r);
                    total += r;
                    got = 0;
                    rounds += 1;
                    if rounds == count { let _ = tx.send(total / count as f64); }
                }
            }).wait().unwrap();
            let avg = rx.recv().unwrap();
            println!("average: {:.1} msg/s", avg);
        }
        "pong" => {
            let p = s.declare_publisher("test/pong").congestion_control(CongestionControl::Block).express(true).wait().unwrap();
            let _sub = s.declare_subscriber("test/ping").callback(move |smp| { p.put(smp.payload().clone()).wait().unwrap(); }).wait().unwrap();
            loop { std::thread::sleep(Duration::from_secs(60)); }
        }
        "ping" => {
            let sub = s.declare_subscriber("test/pong").wait().unwrap();
            let p = s.declare_publisher("test/ping").congestion_control(CongestionControl::Block).express(true).wait().unwrap();
            let data: zenoh::bytes::ZBytes = vec![7u8; a.size].into();
            // wait for the pong side
            loop {
                p.put(data.clone()).wait().unwrap();
                if sub.recv_timeout(Duration::from_millis(200)).unwrap().is_some() { break; }
            }
            let warm = Instant::now();
            while warm.elapsed() < Duration::from_secs(1) {
                p.put(data.clone()).wait().unwrap();
                sub.recv().unwrap();
            }
            let mut rtts = Vec::with_capacity(a.n as usize);
            for _ in 0..a.n {
                let t = Instant::now();
                p.put(data.clone()).wait().unwrap();
                sub.recv().unwrap();
                rtts.push(t.elapsed().as_micros() as u64);
            }
            rtts.sort();
            let avg = rtts.iter().sum::<u64>() as f64 / rtts.len() as f64;
            println!("rtt avg {:.1}µs median {}µs p99 {}µs", avg, rtts[rtts.len() / 2], rtts[rtts.len() * 99 / 100]);
        }
        "router" => loop { std::thread::sleep(Duration::from_secs(60)); },
        c => panic!("unknown command {c:?}"),
    }
}
