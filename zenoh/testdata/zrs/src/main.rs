// zrs: zenoh-rust test peer for tiny-zenoh-go interop.
//   zrs '<json5 config>' sub <key>            -> {"sub":key,"payload":..} per sample
//   zrs '<json5 config>' put <key> <value>    -> put every 200ms, forever
//   zrs '<json5 config>' qbl <key>            -> replies "rust:<payload>" on the query key
//   zrs '<json5 config>' get <selector>       -> {"reply":key,"payload":..}, then {"done":n}
use std::{thread, time::Duration};
use zenoh::{Config, Wait};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cfg = Config::from_json5(&args[1]).expect("config");
    let s = zenoh::open(cfg).wait().expect("open");
    println!("{{\"ready\":\"{}\"}}", s.zid());
    let key = args[3].clone();
    match args[2].as_str() {
        "sub" => {
            let _sub = s
                .declare_subscriber(&key)
                .callback(|smp| {
                    println!(
                        "{{\"sub\":{:?},\"payload\":{:?}}}",
                        smp.key_expr().as_str(),
                        smp.payload().try_to_string().unwrap_or_default().to_string()
                    )
                })
                .wait()
                .unwrap();
            loop {
                thread::sleep(Duration::from_secs(1));
            }
        }
        "put" => loop {
            s.put(&key, args[4].clone()).wait().unwrap();
            thread::sleep(Duration::from_millis(200));
        },
        "qbl" => {
            let _q = s
                .declare_queryable(&key)
                .callback(|q| {
                    let p = q
                        .payload()
                        .map(|p| p.try_to_string().unwrap_or_default().to_string())
                        .unwrap_or_default();
                    q.reply(q.key_expr().clone(), format!("rust:{p}")).wait().unwrap();
                })
                .wait()
                .unwrap();
            loop {
                thread::sleep(Duration::from_secs(1));
            }
        }
        "get" => {
            let replies = s.get(&key).timeout(Duration::from_secs(3)).wait().unwrap();
            let mut n = 0;
            while let Ok(r) = replies.recv() {
                if let Ok(smp) = r.result() {
                    n += 1;
                    println!(
                        "{{\"reply\":{:?},\"payload\":{:?}}}",
                        smp.key_expr().as_str(),
                        smp.payload().try_to_string().unwrap_or_default().to_string()
                    );
                }
            }
            println!("{{\"done\":{n}}}");
        }
        r => panic!("unknown role {r}"),
    }
}
