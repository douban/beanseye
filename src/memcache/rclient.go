/*
 * memcache readonly client
 */

package memcache

import (
    "errors"
    "math"
    "sync"
    "time"
)

type RClient struct {
    scheduler Scheduler
    N, W, R   int
    success   chan bool
}

func NewRClient(sch Scheduler, N, W, R int) (c *RClient) {
    c = new(RClient)
    c.scheduler = sch
    c.N = N
    c.W = W
    c.R = R
    c.success = make(chan bool, 1)
    return c
}

func (c *RClient) Get(key string) (r *Item, targets []string, err error) {
    hosts := c.scheduler.GetHostsByKey(key)
    cnt := 0
    for i, host := range hosts {
        st := time.Now()
        r, err = host.Get(key)
        if err != nil {
            c.scheduler.Feedback(host, key, -10, false)
        } else {
            cnt++
            if r != nil {
                t := float64(time.Now().Sub(st)) / 1e9
                c.scheduler.Feedback(host, key, -float64(math.Sqrt(t)*t), false)
                for j := 0; j < i; j++ {
                    c.scheduler.Feedback(hosts[j], key, -1, false)
                }
                // got the right rval
                targets = []string{host.Addr}
                err = nil
                return
            }
        }
        if cnt >= c.R && i+1 >= c.N {
            err = nil
            break
        }
    }
    return
}

func (c *RClient) getMulti(keys []string) (rs map[string]*Item, targets []string, err error) {
    need := len(keys)
    rs = make(map[string]*Item, need)
    hosts := c.scheduler.GetHostsByKey(keys[0])
    suc := 0
    for i, host := range hosts {
        st := time.Now()
        r, er := host.GetMulti(keys)
        if er != nil { // failed
            err = er
            c.scheduler.Feedback(host, keys[0], -10, false)
        } else {
            suc += 1
            targets = append(targets, host.Addr)
        }

        t := float64(time.Now().Sub(st)) / 1e9
        c.scheduler.Feedback(host, keys[0], -float64(math.Sqrt(t)*t), false)
        for k, v := range r {
            rs[k] = v
        }

        if len(rs) == need {
            break
        }
        if i+1 >= c.N && suc >= c.R {
            err = nil
            targets = []string{}
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

func (c *RClient) GetMulti(keys []string) (rs map[string]*Item, targets []string, err error) {
    var lock sync.Mutex
    rs = make(map[string]*Item, len(keys))

    gs := c.scheduler.DivideKeysByBucket(keys)
    reply := make(chan bool, len(gs))
    for _, ks := range gs {
        if len(ks) > 0 {
            go func(keys []string) {
                r, t, e := c.getMulti(keys)
                if e != nil {
                    err = e
                } else {
                    for k, v := range r {
                        lock.Lock()
                        rs[k] = v
                        targets = append(targets, t...)
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

func (c *RClient) Set(key string, item *Item, noreply bool) (ok bool, targets []string, final_err error) {
    ok = false
    final_err = errors.New("Access Denied for ReadOnly")
    return
}

func (c *RClient) Append(key string, value []byte) (ok bool, targets []string, final_err error) {
    ok = false
    final_err = errors.New("Access Denied for ReadOnly")
    return
}

func (c *RClient) Incr(key string, value int) (result int, target []string, err error) {
    result = 0
    err = errors.New("Access Denied for ReadOnly")
    return
}

func (c *RClient) Delete(key string) (r bool, targets []string, err error) {
    r = false
    err = errors.New("Access Denied for ReadOnly")
    return
}

func (c *RClient) Len() int {
    return 0
}
