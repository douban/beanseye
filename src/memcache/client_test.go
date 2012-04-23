package memcache

import "testing"

var config map[string][]int = map[string][]int{
	"localhost":       []int{0},
	"localhost:11599": []int{0},
}
var badconfig map[string][]int = map[string][]int{
	"localhost:11599": []int{0},
}

func TestClient(t *testing.T) {
	client := NewClient(NewManualScheduler(config))
	client.W = 1
	testStore(t, client)

	client = NewClient(NewManualScheduler(badconfig))
	client.W = 1
	testFailStore(t, client)
}
