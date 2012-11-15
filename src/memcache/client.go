/*
 * memcache client
 */

package memcache

import (
	"errors"
	"math"
	"sync"
	"time"
)

// Client of memcached
type Client struct {
	scheduler Scheduler
	N, W, R   int
}

func NewClient(sch Scheduler) (c *Client) {
	c = new(Client)
	c.scheduler = sch
	c.N = 3
	c.W = 2
	c.R = 1
	return c
}

func (c *Client) Get(key string) (r *Item, err error) {
	hosts := c.scheduler.GetHostsByKey(key)
	cnt := 0
	for i, host := range hosts {
		st := time.Now()
		r, e := host.Get(key)
		if e != nil {
			err = e
			c.scheduler.Feedback(host, key, -10)
		} else {
			cnt++
			if r != nil {
				t := float64(time.Now().Sub(st)) / 1e9
				c.scheduler.Feedback(host, key, -float64(math.Sqrt(t)*t))
				for j := 0; j < i; j++ {
					c.scheduler.Feedback(hosts[j], key, -1)
				}
				return r, nil
			}
		}
		if cnt >= c.R && i+1 >= c.N {
			// because hosts are sorted
			err = nil
			break
		}
	}
	return
}

func (c *Client) getMulti(keys []string) (rs map[string]*Item, err error) {
	need := len(keys)
	rs = make(map[string]*Item, need)
	hosts := c.scheduler.GetHostsByKey(keys[0])
	suc := 0
	for i, host := range hosts {
		st := time.Now()
		r, er := host.GetMulti(keys)
		if er != nil { // failed
			err = er
			c.scheduler.Feedback(host, keys[0], -10)
		} else {
			suc += 1
		}

		t := float64(time.Now().Sub(st)) / 1e9
		c.scheduler.Feedback(host, keys[0], -float64(math.Sqrt(t)*t))
		for k, v := range r {
			rs[k] = v
		}

		if len(rs) == need {
			break
		}
		if i+1 >= c.N && suc >= c.R {
			err = nil
			break
		}

		new_keys := []string{}
		for _, k := range keys {
			if _, ok := rs[k]; !ok {
				new_keys = append(new_keys, k)
			}
		}
		keys = new_keys
		if len(keys) == 0 {
			break // repeated keys
		}
	}
	if len(rs) > 0 {
		err = nil
	}
	return
}

func (c *Client) GetMulti(keys []string) (rs map[string]*Item, err error) {
	var lock sync.Mutex
	rs = make(map[string]*Item, len(keys))

	gs := c.scheduler.DivideKeysByBucket(keys)
	reply := make(chan bool, len(gs))
	for _, ks := range gs {
		if len(ks) > 0 {
			go func(keys []string) {
				r, e := c.getMulti(keys)
				if e != nil {
					err = e
				} else {
					for k, v := range r {
						lock.Lock()
						rs[k] = v
						lock.Unlock()
					}
				}
				reply <- true
			}(ks)
		} else {
			reply <- true
		}
	}
	// wait for complete
	for _, _ = range gs {
		<-reply
	}
	return
}

func (c *Client) Set(key string, item *Item, noreply bool) (bool, error) {
	suc := 0
	for i, host := range c.scheduler.GetHostsByKey(key) {
		if ok, err := host.Set(key, item, noreply); err == nil && ok {
			suc++
		} else {
			c.scheduler.Feedback(host, key, -2)
		}
		if suc >= c.W && (i+1) >= c.N {
			break
		}
	}
	if suc == 0 {
		return false, errors.New("write failed")
	}
	return suc >= c.W, nil
}

func (c *Client) Append(key string, value []byte) (bool, error) {
	suc := 0
	for i, host := range c.scheduler.GetHostsByKey(key) {
		if ok, err := host.Append(key, value); err == nil && ok {
			suc++
		}
		if suc >= c.W && (i+1) >= c.N {
			break
		}
	}
	if suc == 0 {
		return false, errors.New("write failed")
	}
	return suc >= c.W, nil
}

func (c *Client) Incr(key string, value int) (int, error) {
	result := 0
	suc := 0
	var err error
	for i, host := range c.scheduler.GetHostsByKey(key) {
		r, e := host.Incr(key, value)
		if e != nil {
			err = e
			continue
		}
		if r > 0 {
			suc++
		}
		if r > result {
			result = r
		}
		if suc >= c.W && (i+1) >= c.N {
			break
		}
	}
	if result > 0 {
		err = nil
	}
	return result, err // maximize
}

func (c *Client) Delete(key string) (r bool, err error) {
	suc := 0
	for _, host := range c.scheduler.GetHostsByKey(key) {
		ok, er := host.Delete(key)
		if er != nil {
			err = er
		} else if ok {
			suc++
		}
		if suc >= c.N {
			break
		}
	}
	if suc > 0 {
		err = nil
	}
	return suc >= c.W, err
}

func (c *Client) Len() int {
	return 0
}
