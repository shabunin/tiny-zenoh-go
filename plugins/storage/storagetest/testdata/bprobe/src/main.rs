// bprobe drives a zenoh-rust storage backend directly:
//   bprobe <influxdb|influxdb2|s3> '<storage_manager plugin config JSON>' <op> [args]
// ops: put <key|-> <payload> <encoding>; del <key|->; get <key|-> <params>; entries
// The config must have one volume and one storage.
use std::time::{SystemTime, UNIX_EPOCH};

use zenoh::{
    bytes::Encoding,
    key_expr::OwnedKeyExpr,
    time::{Timestamp, TimestampId, NTP64},
};
use zenoh_backend_traits::config::PluginConfig;
use zenoh_plugin_trait::Plugin;

fn key(s: &str) -> Option<OwnedKeyExpr> {
    if s == "-" {
        None
    } else {
        Some(OwnedKeyExpr::try_from(s.to_string()).unwrap())
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let backend = args[1].as_str();
    let cfg: serde_json::Value = serde_json::from_str(&args[2]).unwrap();
    let pc = PluginConfig::try_from(("storage_manager", &cfg)).unwrap();
    let vc = &pc.volumes[0];
    let sc = pc.storages[0].clone();
    let vol = match backend {
        "influxdb" => zenoh_backend_influxdb::InfluxDbBackend::start("influxdb", vc).unwrap(),
        "influxdb2" => zenoh_backend_influxdb2::InfluxDbBackend::start("influxdb2", vc).unwrap(),
        "s3" => zenoh_backend_s3::S3Backend::start("s3", vc).unwrap(),
        _ => panic!("backend"),
    };
    let mut st = vol.create_storage(sc).await.unwrap();
    match args[3].as_str() {
        "put" => {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let ts = Timestamp::new(NTP64::from(now), TimestampId::try_from([0x77, 0x33]).unwrap());
            let r = st
                .put(key(&args[4]), args[5].clone().into(), Encoding::from(args[6].clone()), ts)
                .await
                .unwrap();
            println!("{{\"result\":\"{r:?}\",\"timestamp\":\"{ts}\"}}");
        }
        "del" => {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let ts = Timestamp::new(NTP64::from(now), TimestampId::try_from([0x77, 0x33]).unwrap());
            let r = st.delete(key(&args[4]), ts).await.unwrap();
            println!("{{\"result\":\"{r:?}\"}}");
        }
        "get" => {
            for d in st.get(key(&args[4]), &args[5]).await.unwrap() {
                println!(
                    "{}",
                    serde_json::json!({
                        "payload": String::from_utf8_lossy(&d.payload.to_bytes()),
                        "encoding": d.encoding.to_string(),
                        "timestamp": d.timestamp.to_string(),
                    })
                );
            }
        }
        "entries" => {
            for (k, ts) in st.get_all_entries().await.unwrap() {
                println!(
                    "{}",
                    serde_json::json!({"key": k.map(|k| k.to_string()).unwrap_or("-".into()), "timestamp": ts.to_string()})
                );
            }
        }
        _ => panic!("op"),
    }
    drop(st);
    drop(vol);
    std::process::exit(0);
}
