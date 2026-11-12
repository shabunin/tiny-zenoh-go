//go:build !tinygo

package transport

// txDefaultMode: transmission queues, as zenoh-rust.
const txDefaultMode = TxQueued

// rxBuffered: stream links are read through a buffer, so that one read
// brings in several batches (and their length prefixes) instead of two
// reads per batch. Off on TinyGo, where the memory counts more.
const rxBuffered = true
