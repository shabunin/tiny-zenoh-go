//go:build tinygo

package transport

// txDefaultMode: on TinyGo, Send writes batches itself (no goroutine and
// batch buffers per link).
const txDefaultMode = TxSync
