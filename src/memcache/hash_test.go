package memcache

import (
	"testing"
)

type TestCase struct {
	name  string
	value string
	hash  uint32
}

var tests []TestCase = []TestCase{
	TestCase{"md5", "", 3649838548},
	TestCase{"crc32", "", 0},
	TestCase{"fnv1a", "", 2166136261},
	TestCase{"fnv1a1", "", 2166136261},

	TestCase{"md5", "hello", 708854109},
	TestCase{"crc32", "hello", 907060870},
	TestCase{"fnv1a", "hello", 1335831723},
	TestCase{"fnv1a1", "hello", 1335831723},

	TestCase{"md5", "你好", 2674444926},
	TestCase{"crc32", "你好", 1352841281},
	TestCase{"fnv1a", "你好", 2257816995},
	TestCase{"fnv1a1", "你好", 718964643},
}

func Test(t *testing.T) {
	for _, c := range tests {
		h := hashMethods[c.name]([]byte(c.value))
		if c.hash != h {
			t.Errorf("test hash faile: %s(%s)(%d) != %d", c.name, c.value, h, c.hash)
		}
	}
}
