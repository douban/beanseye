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
}

func NewRClient(sch Scheduler, N, W, R int) (c *RClient) {
    c = new(RClient)
    c.scheduler = sch
    c.N = N
    c.W = W
    c.R = R
    return c
}

func (c *RClient) Get(key string) (r *Item, targets []string, err error) {
    hosts := c.scheduler.GetHostsByKey(key)
    cnt := 0
    for _, host := range hosts {
        st := time.Now()
        r, err = host.Get(key)
        if err == nil {
            cnt++
            if r != nil {
                t := float64(time.Now().Sub(st)) / 1e9
                c.scheduler.Feedback(host, key, 1 - float64(math.Sqrt(t)*t))
                // got the right rval
                targets = []string{host.Addr}
                err = nil
                //return r, nil
                return
            }
        } else if err.Error() != "wait for retry" {
            c.scheduler.Feedback(host, key, -5)
        } else {
            c.scheduler.Feedback(host, key, -2)
        }

        if cnt >= c.R {
            // because hosts are sorted
            err = nil
        }
    }
    // here is a failure exit
    return
}

func (c *RClient) getMulti(keys []string) (rs map[string]*Item, targets []string, err error) {
    need := len(keys)
    rs = make(map[string]*Item, need)
    hosts := c.scheduler.GetHostsByKey(keys[0])
    suc := 0
    for _, host := range hosts {
        st := time.Now()
        r, er := host.GetMulti(keys)
        if er == nil {
            suc += 1
            if r != nil {
                targets = append(targets, host.Addr)
                t := float64(time.Now().Sub(st)) / 1e9
                c.scheduler.Feedback(host, keys[0], 1 - float64(math.Sqrt(t)*t))
            }
        } else if er.Error() != "wait for retry" { // failed
            c.scheduler.Feedback(host, keys[0], -5)
        } else {
            c.scheduler.Feedback(host, keys[0], -2)
        }
        err = er
        if er != nil {
            continue
        }

        for k, v := range r {
            rs[k] = v
        }

        if len(rs) == need {
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
    if suc > c.R {
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
