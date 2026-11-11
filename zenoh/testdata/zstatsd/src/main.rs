// A zenoh-rust 1.10.2 router built with the "stats" feature, for comparing
// tiny-zenoh-go's admin-space metrics with zenohd's.
// usage: zstatsd '<json config inserts>'   (prints its zid, then runs)
use zenoh::Config;

#[tokio::main]
async fn main() {
    let arg = std::env::args().nth(1).unwrap_or_else(|| "{}".into());
    let inserts: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&arg).unwrap();
    let mut conf = Config::default();
    conf.insert_json5("mode", "\"router\"").unwrap();
    conf.insert_json5("adminspace/enabled", "true").unwrap();
    for (k, v) in inserts {
        conf.insert_json5(&k, &v.to_string()).unwrap();
    }
    let s = zenoh::open(conf).await.unwrap();
    println!("{}", s.zid());
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}
