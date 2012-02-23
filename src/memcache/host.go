package memcache

import (
	"bufio"
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

var MaxFreeConns = 20

type Host struct {
	Addr     string
	nextDial time.Time
	conns    chan net.Conn
}

func NewHost(addr string) *Host {
	host := &Host{Addr: addr}
	host.conns = make(chan net.Conn, MaxFreeConns)
	return host
}

// Given a string of the form "host", "host:port", or "[ipv6::address]:port",
// return true if the string includes a port.
func hasPort(s string) bool { return strings.LastIndex(s, ":") > strings.LastIndex(s, "]") }

func (host *Host) Close() {
	// first := host.getConn()
	// host.releaseConn(first)
	// for conn := host.getConn() {
	//     conn.Close()
	//     if conn == first {
	//         break // last conn
	//     }
	// }
}

func (host *Host) createConn() (net.Conn, error) {
	now := time.Now()
	if host.nextDial.After(now) {
		return nil, errors.New("wait for retry")
	}

	addr := host.Addr
	if !hasPort(addr) {
		addr = addr + ":11211"
	}
	conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond) // 300ms
	if err != nil {
		host.nextDial = now.Add(time.Second*10)
		return nil, err
	}
    // FIXME: should use SetDeadline() before read and write
	// conn.SetTimeout(time.Second*3) // timeout 3s
	return conn, nil
}

func (host *Host) getConn() (c net.Conn, err error) {
	select {
	case c = <-host.conns:
	default:
		c, err = host.createConn()
	}
	return
}

func (host *Host) releaseConn(conn net.Conn) {
	select {
	case host.conns <- conn:
	default:
		conn.Close()
	}
}

func (host *Host) execute(req *Request) (resp *Response, err error) {
	var conn net.Conn
	conn, err = host.getConn()
	if err != nil {
		return
	}

	err = req.Write(conn)
	if err != nil {
		log.Print("write request failed:", err)
		conn.Close()
		return
	}

	resp = new(Response)
	if req.NoReply {
		host.releaseConn(conn)
		resp.status = "STORED"
		return
	}

	reader := bufio.NewReader(conn)
	err = resp.Read(reader)
	if err != nil {
		log.Print("read response failed:", err)
		conn.Close()
		return
	}

	if err := req.Check(resp); err != nil {
		log.Print("unexpected response", req, resp, err)
		conn.Close()
		return nil, err
	}

	host.releaseConn(conn)
	return
}

func (host *Host) Get(key string) (*Item, error) {
	req := &Request{Cmd: "get", Keys: []string{key}}
	resp, err := host.execute(req)
	if err != nil {
		return nil, err
	}
	item, _ := resp.items[key]
	return item, nil
}

func (host *Host) GetMulti(keys []string) (map[string]*Item, error) {
	req := &Request{Cmd: "get", Keys: keys}
	resp, err := host.execute(req)
	if err != nil {
		return nil, err
	}
	return resp.items, nil
}

func (host *Host) store(cmd string, key string, item *Item, noreply bool) (bool, error) {
	req := &Request{Cmd: cmd, Keys: []string{key}, Item: item, NoReply: noreply}
	resp, err := host.execute(req)
	return err == nil && resp.status == "STORED", err
}

func (host *Host) Set(key string, item *Item, noreply bool) (bool, error) {
	return host.store("set", key, item, noreply)
}

func (host *Host) Append(key string, value []byte) (bool, error) {
	req := &Request{Cmd: "append", Keys: []string{key}, Item: &Item{Body: value}}
	resp, err := host.execute(req)
	return err == nil && resp.status == "STORED", err
}

func (host *Host) Incr(key string, value int) (int, error) {
	req := &Request{Cmd: "incr", Keys: []string{key}, Item: &Item{Body: []byte(strconv.Itoa(value))}}
	resp, err := host.execute(req)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(resp.msg)
}

func (host *Host) Delete(key string) (bool, error) {
	req := &Request{Cmd: "delete", Keys: []string{key}}
	resp, err := host.execute(req)
	return err == nil && resp.status == "DELETED", err
}

func (host *Host) Stat(keys []string) (map[string]string, error) {
	req := &Request{Cmd: "stats", Keys: keys}
	resp, err := host.execute(req)
	if err != nil {
		return nil, err
	}
	st := make(map[string]string)
	for key, item := range resp.items {
		st[key] = string(item.Body)
	}
	return st, nil
}

func (host *Host) Len() int {
	return 0
}
