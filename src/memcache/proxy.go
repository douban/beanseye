package memcache

func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}

func NewProxy(config map[string][]int, auto bool) *Server {
	var sch Scheduler
	if auto {
		servers := make([]string, 0)
		for s, _ := range config {
			servers = append(servers, s)
		}
		sch = NewAutoScheduler(servers, 16)
	} else {
		sch = NewManualScheduler(config)
	}
	client := NewClient(sch)
	n := len(config)
	client.N = min(n, 3)
	client.W = min(n, 2)
	return NewServer(client)
}

type ReadClient struct {
	rw, ro Storage
}

func NewReadClient(rw, ro Storage) *ReadClient {
	return &ReadClient{rw: rw, ro: ro}
}

func (p *ReadClient) Get(key string) (*Item, error) {
	r, err := p.rw.Get(key)
	if err != nil {
		return nil, err
	}
	if r == nil {
		r, err = p.ro.Get(key)
		if err != nil {
			return nil, err
		}
		if r != nil {
			p.rw.Set(key, r, false)
		}
	}
	return r, nil
}

func (p *ReadClient) GetMulti(keys []string) (map[string]*Item, error) {
	rs, err := p.rw.GetMulti(keys)
	if err != nil {
		return nil, err
	}
	if len(rs) < len(keys) {
		keys2 := []string{}
		for _, key := range keys {
			if _, ok := rs[key]; !ok {
				keys2 = append(keys2, key)
			}
		}
		r, err := p.ro.GetMulti(keys2)
		if err != nil {
			return nil, err
		}
		if r != nil {
			for k, v := range r {
				rs[k] = v
				p.rw.Set(k, v, true)
			}
		}
	}
	return rs, nil
}

func (p *ReadClient) Set(key string, item *Item, noreply bool) (bool, error) {
	return p.rw.Set(key, item, noreply)
}

func (p *ReadClient) Append(key string, value []byte) (bool, error) {
	return p.rw.Append(key, value)
}

func (p *ReadClient) Incr(key string, value int) (int, error) {
	return p.rw.Incr(key, value)
}

func (p *ReadClient) Delete(key string) (bool, error) {
	return p.rw.Set(key, &Item{Body: []byte("")}, false)
}

func (p *ReadClient) Len() int {
	return 0
}

func NewReadProxy(rw_server, ro_server string) *Server {
	proxy := NewReadClient(NewHost(rw_server), NewHost(ro_server))
	return NewServer(proxy)
}
