package shm

import (
	"os"
	"syscall"
	"testing"
)

// A segment whose name was unlinked by another cleanup, and then reused by a
// new segment, must not remove the new one when it closes (zenoh-rust #2873).
func TestCloseKeepsReplacement(t *testing.T) {
	a, err := createSegment(1<<32-1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	path := segPath(a.id)
	if err := os.Remove(path); err != nil { // another process's cleanup won
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, 4096), 0o600); err != nil { // a new segment, same id
		t.Fatal(err)
	}
	defer os.Remove(path)
	a.close()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("replacement removed: %v", err)
	}
}

// Opening a segment that was unlinked after our open but before our lock
// fails instead of mapping a detached inode.
func TestOpenUnlinkedSegment(t *testing.T) {
	a, err := createSegment(1<<32-1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	fd, err := syscall.Open(segPath(a.id), syscall.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	if unlinked(fd) {
		t.Fatal("linked segment reported unlinked")
	}
	if err := os.Remove(segPath(a.id)); err != nil {
		t.Fatal(err)
	}
	if !unlinked(fd) {
		t.Fatal("unlinked segment not detected")
	}
}
