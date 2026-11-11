//go:build linux && !tinygo

package shm

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

// POSIX shared-memory segments as zenoh-rust's shm/unix.rs makes them:
// shm_open("<id>.zenoh") (i.e. /dev/shm/<id>.zenoh on Linux), mode 0600,
// a shared flock held for as long as the segment is mapped, mmap
// MAP_SHARED|MAP_NORESERVE. Whoever unmaps last (gets the lock exclusively)
// unlinks the file.

const devShm = "/dev/shm/"

var errSegmentGone = errors.New("shm: segment is invalid")

type segment struct {
	id  uint64
	fd  int
	mem []byte
}

func segPath(id uint64) string { return devShm + strconv.FormatUint(id, 10) + ".zenoh" }

// createSegment makes a new segment of size bytes under a random id in
// [0, maxID].
func createSegment(maxID uint64, size int) (*segment, error) {
	cleanupOrphans()
	for range 100 {
		id := rand.Uint64N(maxID + 1)
		path := segPath(id)
		fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
		if err == syscall.EEXIST {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("shm: create %s: %w", path, err)
		}
		if err := syscall.Flock(fd, syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
			_ = syscall.Unlink(path)
			_ = syscall.Close(fd)
			continue
		}
		if err := syscall.Ftruncate(fd, int64(size)); err != nil {
			_ = syscall.Unlink(path)
			_ = syscall.Close(fd)
			return nil, fmt.Errorf("shm: size %s: %w", path, err)
		}
		mem, err := mapFD(fd, size)
		if err != nil {
			_ = syscall.Unlink(path)
			_ = syscall.Close(fd)
			return nil, err
		}
		return &segment{id: id, fd: fd, mem: mem}, nil
	}
	return nil, errors.New("shm: no free segment id after 100 tries")
}

// openSegment maps another process's segment.
func openSegment(id uint64) (*segment, error) {
	path := segPath(id)
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("shm: open %s: %w", path, err)
	}
	if err := syscall.Flock(fd, syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		_ = syscall.Close(fd)
		return nil, errSegmentGone // being torn down by its last user
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil || st.Size <= 0 || st.Size > 1<<40 {
		_ = syscall.Close(fd)
		return nil, errSegmentGone
	}
	mem, err := mapFD(fd, int(st.Size))
	if err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	return &segment{id: id, fd: fd, mem: mem}, nil
}

func mapFD(fd, size int) ([]byte, error) {
	mem, err := syscall.Mmap(fd, 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_NORESERVE)
	if err != nil {
		return nil, fmt.Errorf("shm: mmap: %w", err)
	}
	// zenoh-rust mlocks its segments; we try, but a low RLIMIT_MEMLOCK only
	// costs performance, not correctness.
	_ = syscall.Mlock(mem)
	return mem, nil
}

// close unmaps the segment and unlinks it if nobody else has it open.
func (s *segment) close() {
	if s.mem != nil {
		_ = syscall.Munmap(s.mem)
		s.mem = nil
	}
	if syscall.Flock(s.fd, syscall.LOCK_EX|syscall.LOCK_NB) == nil {
		_ = syscall.Unlink(segPath(s.id))
	}
	_ = syscall.Close(s.fd)
}

func (s *segment) u32(off int) *uint32 { return (*uint32)(unsafe.Pointer(&s.mem[off])) }
func (s *segment) u64(off int) *uint64 { return (*uint64)(unsafe.Pointer(&s.mem[off])) }

var orphanOnce sync.Once

// cleanupOrphans removes segments left behind by processes that died without
// unlinking them: nobody holds their lock any more (as zenoh-rust does on
// start-up).
func cleanupOrphans() {
	orphanOnce.Do(func() {
		ents, err := os.ReadDir(devShm)
		if err != nil {
			return
		}
		for _, e := range ents {
			stem, ok := strings.CutSuffix(e.Name(), ".zenoh")
			if !ok {
				continue
			}
			id, err := strconv.ParseUint(stem, 10, 64)
			if err != nil {
				continue
			}
			fd, err := syscall.Open(segPath(id), syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
			if err != nil {
				continue
			}
			if syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB) == nil {
				_ = syscall.Unlink(segPath(id))
			}
			_ = syscall.Close(fd)
		}
	})
}
