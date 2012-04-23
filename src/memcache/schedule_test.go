package memcache

import "testing"

type testCase struct {
	key   string
	hosts []string
}

func testScheduler(t *testing.T, schd Scheduler, cases []testCase, in_order bool) {
	for i, c := range cases {
		hosts := schd.GetHostsByKey(c.key)
		t.Log(i, c.key, c.hosts, hosts)
		if len(hosts) != len(c.hosts) {
			t.Errorf("case #%d: key %s: number of hosts not match, %d <> %d", i, c.key, len(hosts), len(c.hosts))
			continue
		}
		if in_order || len(hosts) == 1 {
			for j, h := range hosts {
				if h.Addr != c.hosts[j] {
					t.Errorf("key %s: expect %s but %s", c.key, c.hosts[j], h.Addr)
				}
			}
		} else {
			m := make(map[string]bool, len(c.hosts))
			for _, h := range c.hosts {
				m[h] = true
			}
			for _, h := range hosts {
				if _, ok := m[h.Addr]; !ok {
					t.Errorf("key %s: host %s is not expected", c.key, h.Addr)
				}
			}
		}
	}
}

var mhosts = map[string][]int{
	"host1": {0, 1},
	"host2": {2, 3},
	"host3": {1, 3},
}

var mtests = []testCase{
	testCase{"1key1", []string{"host2", "host3"}},
	testCase{"2key2", []string{"host1"}},
	testCase{"3key3", []string{"host1"}},
	testCase{"4key4", []string{"host1", "host3"}},
}

func TestManualScheduler(t *testing.T) {
	schd := NewManualScheduler(mhosts)
	testScheduler(t, schd, mtests, false)
}

func TestAutoScheduler(t *testing.T) {
}

var modhosts = []string{
	"host0:11211", "host0:11212", "host0:11213", "host0:11214",
	"host1:11211", "host1:11212", "host1:11213", "host1:11214",
}

var modtests = []testCase{
	testCase{"0:key:0", []string{"host1:11211"}},
	testCase{"1:key:1", []string{"host1:11213"}},
	testCase{"2:key:2", []string{"host1:11213"}},
	testCase{"3:key:3", []string{"host0:11211"}},
}

func TestModScheduler(t *testing.T) {
	schd := NewModScheduler(modhosts, "md5")
	testScheduler(t, schd, modtests, true)
}

var chthosts = []string{
	"host0:11211", "host0:11212", "host0:11213", "host0:11214",
	"host1:11211", "host1:11212", "host1:11213", "host1:11214",
	"host2:11211", "host2:11212", "host2:11213", "host2:11214",
	"host3:11211", "host3:11212", "host3:11213", "host3:11214",
	"host4:11211", "host4:11212", "host4:11213", "host4:11214",
	"host5:11211", "host5:11212", "host5:11213", "host5:11214",
	"host6:11211", "host6:11212", "host6:11213", "host6:11214",
	"host7:11211", "host7:11212", "host7:11213", "host7:11214",
	"host8:11211", "host8:11212", "host8:11213", "host8:11214",
	"host9:11211", "host9:11212", "host9:11213", "host9:11214",
}

var chtests = []testCase{
	testCase{"key:0", []string{"host2:11214"}},
	testCase{"key:1", []string{"host5:11213"}},
	testCase{"key:2", []string{"host0:11213"}},
	testCase{"key:3", []string{"host6:11212"}},
	testCase{"key:4", []string{"host4:11214"}},
	testCase{"key:5", []string{"host2:11213"}},
	testCase{"key:6", []string{"host1:11211"}},
	testCase{"key:7", []string{"host0:11214"}},
	testCase{"key:8", []string{"host0:11214"}},
	testCase{"key:9", []string{"host5:11212"}},
	testCase{"key:10", []string{"host4:11214"}},
	testCase{"key:11", []string{"host2:11212"}},
	testCase{"key:12", []string{"host7:11212"}},
	testCase{"key:13", []string{"host8:11213"}},
	testCase{"key:14", []string{"host8:11212"}},
	testCase{"key:15", []string{"host2:11213"}},
	testCase{"key:16", []string{"host8:11212"}},
	testCase{"key:17", []string{"host1:11212"}},
	testCase{"key:18", []string{"host2:11214"}},
	testCase{"key:19", []string{"host8:11212"}},
	testCase{"key:20", []string{"host1:11211"}},
	testCase{"key:21", []string{"host1:11211"}},
	testCase{"key:22", []string{"host6:11211"}},
	testCase{"key:23", []string{"host1:11213"}},
	testCase{"key:24", []string{"host3:11212"}},
	testCase{"key:25", []string{"host8:11213"}},
	testCase{"key:26", []string{"host7:11213"}},
	testCase{"key:27", []string{"host3:11212"}},
	testCase{"key:28", []string{"host0:11212"}},
	testCase{"key:29", []string{"host2:11212"}},
	testCase{"key:30", []string{"host8:11213"}},
	testCase{"key:31", []string{"host8:11211"}},
	testCase{"key:32", []string{"host7:11213"}},
	testCase{"key:33", []string{"host1:11213"}},
	testCase{"key:34", []string{"host9:11212"}},
	testCase{"key:35", []string{"host4:11214"}},
	testCase{"key:36", []string{"host3:11213"}},
	testCase{"key:37", []string{"host7:11211"}},
	testCase{"key:38", []string{"host3:11212"}},
	testCase{"key:39", []string{"host9:11213"}},
	testCase{"key:40", []string{"host1:11212"}},
	testCase{"key:41", []string{"host3:11214"}},
	testCase{"key:42", []string{"host0:11211"}},
	testCase{"key:43", []string{"host9:11213"}},
	testCase{"key:44", []string{"host1:11212"}},
	testCase{"key:45", []string{"host4:11213"}},
	testCase{"key:46", []string{"host5:11212"}},
	testCase{"key:47", []string{"host4:11214"}},
	testCase{"key:48", []string{"host8:11213"}},
	testCase{"key:49", []string{"host3:11214"}},
}

func TestConsistantHashScheduler(t *testing.T) {
	schd := NewConsistantHashScheduler(chthosts, "md5")
	testScheduler(t, schd, chtests, true)
}
