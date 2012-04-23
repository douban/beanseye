package memcache

import (
	"testing"
)

func TestHost(t *testing.T) {
	host := NewHost("localhost")
	testStore(t, host)
	st, err := host.Stat(nil)
	if err != nil {
		t.Error("stat fail", err)
	}
	if up, ok := st["uptime"]; !ok || len(up) < 1 {
		t.Error("no uptime in stat", st)
	}
	testFailStore(t, NewHost("localhost:11911"))
}
