package storage_test

import (
	"testing"

	"github.com/shabunin/tiny-zenoh-go/plugins/storage"
	"github.com/shabunin/tiny-zenoh-go/plugins/storage/storagetest"
)

func TestMemoryBackendContract(t *testing.T) {
	storagetest.TestBackend(t, storage.NewMemoryBackend(), storage.MemoryVolume{}.Capability(), "m")
	storagetest.TestStorage(t, storage.MemoryVolume{}, nil, "memcontract")
}

func TestTimestampText(t *testing.T) {
	ts, err := storage.ParseTimestamp("7386690599959157260/33")
	if err != nil || ts.Time != 7386690599959157260 || ts.ID.String() != "33" || storage.FormatTimestamp(ts) != "7386690599959157260/33" {
		t.Errorf("%+v %v", ts, err)
	}
	for _, bad := range []string{"", "12", "x/1", "1/", "1/0g"} {
		if _, err := storage.ParseTimestamp(bad); err == nil {
			t.Errorf("parsed %q", bad)
		}
	}
	if _, err := storage.OpenVolume("nope", "v", nil); err == nil {
		t.Error("unknown backend opened")
	}
}
