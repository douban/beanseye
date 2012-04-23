package memcache

import (
	"net"
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	s, _ := StartServer("localhost:11299")
	time.Sleep(1e8)
	client := NewClient(NewManualScheduler(map[string][]int{"localhost:11299": []int{0}}))
	client.W = 1

	testStore(t, client)
	s.Shutdown()
}

func TestShutdown(t *testing.T) {
	addr := "localhost:11298"
	s, _ := StartServer(addr)
	go func() {
		time.Sleep(1e8)
		s.Shutdown()
	}()
	if _, err := net.Dial("tcp", addr); err != nil {
		t.Error("server fail")
	}
	time.Sleep(2e8) // wait for close
	if _, err := net.Dial("tcp", addr); err == nil {
		t.Error("server not shundown")
	}

}
