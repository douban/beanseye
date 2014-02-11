package memcache

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var SlowCmdTime = time.Millisecond * 100 // 100ms

type ServerConn struct {
	RemoteAddr      string
	rwc             io.ReadWriteCloser // i/o connection
	closeAfterReply bool
}

func newServerConn(conn net.Conn) *ServerConn {
	c := new(ServerConn)
	c.RemoteAddr = conn.RemoteAddr().String()
	c.rwc = conn
	return c
}

func (c *ServerConn) Close() {
	if c.rwc != nil {
		c.rwc.Close()
		c.rwc = nil
	}
}

func (c *ServerConn) Shutdown() {
	c.closeAfterReply = true
}

func (c *ServerConn) Serve(store DistributeStorage, stats *Stats) (e error) {
	rbuf := bufio.NewReader(c.rwc)
	wbuf := bufio.NewWriter(c.rwc)

	req := new(Request)
	for {
		e = req.Read(rbuf)
		if e != nil {
			break
		}

		t := time.Now()
		var err error
		resp, hosts, err := req.Process(store, stats)
		if resp == nil {
			break
		}
		dt := time.Since(t)
		if dt > SlowCmdTime {
			stats.UpdateStat("slow_cmd", 1)
		}

		if !resp.noreply {
			if resp.Write(wbuf) != nil || wbuf.Flush() != nil {
				break
			}
		}

		if AccessLog != nil {
			key := strings.Join(req.Keys, ":")
			size := 0
			switch req.Cmd {
			case "get", "gets":
				for _, v := range resp.items {
					size += len(v.Body)
				}
			case "set", "add", "replace":
				size = len(req.Item.Body)
			}
			if err != nil {
				size = -1
			}
			if len(hosts) == 0 {
				hosts = append(hosts, "NoWhere")
			}
			var hosts_str string
			if req.Cmd == "get" && size == 0 {
				hosts_str = fmt.Sprintf("FAILED with %s", strings.Join(hosts, ","))
			} else {
				hosts_str = fmt.Sprintf("from %s", strings.Join(hosts, ","))
			}
			AccessLog.Printf("%s %s %s %d %s %dms", c.RemoteAddr, req.Cmd, key, size, hosts_str, dt.Nanoseconds()/1e6)
		}

		req.Clear()
		resp.CleanBuffer()

		if c.closeAfterReply {
			break
		}
	}
	c.Close()
	return
}

type Server struct {
	sync.Mutex
	addr  string
	l     net.Listener
	store DistributeStorage
	conns map[string]*ServerConn
	stats *Stats
	stop  bool
}

func NewServer(store DistributeStorage) *Server {
	s := new(Server)
	s.store = store
	s.conns = make(map[string]*ServerConn, 1024)
	s.stats = NewStats()
	return s
}

func (s *Server) Listen(addr string) (e error) {
	s.addr = addr
	s.l, e = net.Listen("tcp", addr)
	return
}

func (s *Server) Serve() (e error) {
	if s.l == nil {
		return errors.New("no listener")
	}

	// trap signal
	sch := make(chan os.Signal, 10)
	signal.Notify(sch, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT,
		syscall.SIGHUP, syscall.SIGSTOP, syscall.SIGQUIT)
	go func(ch <-chan os.Signal) {
        switch sig := <-ch; sig {
        case syscall.SIGINT: // Ctrl+C
            OpenAccessLog(AccessLogPath)
            OpenErrorLog(ErrorLogPath)
        default:
		    ErrorLog.Print("signal recieved " + sig.String())
            AccessFd.Close()
            ErrorFd.Close()
		    s.Shutdown()
        }
	}(sch)

	// log.Print("start serving at ", s.addr, "...\n")
	for {
		rw, e := s.l.Accept()
		if e != nil {
			ErrorLog.Print("Accept failed: ", e)
			return e
		}
		if s.stop {
			break
		}
		c := newServerConn(rw)
		go func() {
			s.Lock()
			s.conns[c.RemoteAddr] = c
			s.stats.curr_connections++
			s.stats.total_connections++
			s.Unlock()

			c.Serve(s.store, s.stats)

			s.Lock()
			s.stats.curr_connections--
			delete(s.conns, c.RemoteAddr)
			s.Unlock()
		}()
	}
	s.l.Close()
	// wait for connections to close
	for i := 0; i < 20; i++ {
		s.Lock()
		if len(s.conns) == 0 {
			return nil
		}
		s.Unlock()
		time.Sleep(1e8)
	}
	ErrorLog.Print("shutdown ", s.addr, "\n")
	return nil
}

func (s *Server) Shutdown() {
	s.stop = true

	// try to connect
	// net.Dial("tcp", s.addr)

	// notify conns
	s.Lock()
    defer s.Unlock()
	if len(s.conns) > 0 {
		// log.Print("have ", len(s.conns), " active connections")
		for _, conn := range s.conns {
			// log.Print(s)
			conn.Shutdown()
		}
	}
	//s.Unlock()
}

/*
func StartServer(addr string) (*Server, error) {
	store := NewMapStore()
	s := NewServer(store)
	e := s.Listen(addr)
	if e != nil {
		return nil, e
	}
	go func() {
		s.Serve()
	}()
	return s, nil
}
*/
