package memcache

import (
	"testing"
	"time"
	//    "os"
)

func testProxy(t *testing.T, auto bool) {
	//AccessLog = log.New(os.Stdout, nil, "", log.Ldate|log.Ltime)
	s1, _ := StartServer("localhost:11297")
	defer s1.Shutdown()
	s2, _ := StartServer("localhost:11296")
	defer s2.Shutdown()

	config := map[string][]int{
		"localhost:11296": []int{0},
		"localhost:11297": []int{0},
	}
	addr := "localhost:11295"
	p := NewProxy(config, auto)
	p.Listen(addr)
	defer p.Shutdown()
	go func() {
		p.Serve()
	}()
	time.Sleep(1e8)

	client := NewHost(addr)
	testStore(t, client)
}

func TestProxy(t *testing.T) {
	testProxy(t, false)
	time.Sleep(1e9)
	testProxy(t, true)
}
