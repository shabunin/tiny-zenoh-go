# zstatsd

A zenoh-rust 1.10.2 router built with the `stats` feature (zenohd releases
and the eclipse-zenoh Python wheel are built without it), for
`TestStatsRust`: it takes JSON config inserts as its argument and prints its
ZID.

    cargo build --release
    ZENOHD_STATS=$PWD/target/release/zstatsd go test ./zenoh -run TestStatsRust

zenoh 1.10.2 asks for webpki-roots 1.0.10, which crates.io no longer
resolves; if cargo fails on it, copy the `Cargo.lock` of the zenoh 1.10.2
source tree next to this `Cargo.toml` first (locked versions still build).
