package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/kless/goconfig/config"
	"log"
	. "memcache"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
)

var conf *string = flag.String("conf", "conf/example.ini", "config path")
var debug *bool = flag.Bool("debug", false, "debug info")
var allocLimit *int = flag.Int("alloc", 1024*4, "cmem alloc limit")

func in(s, subs interface{}) (bool, error) {
	return strings.Contains(s.(string), subs.(string)), nil
}

func timer(v interface{}) string {
	t := v.(uint64)
	switch {
	case t > 3600*24*2:
		return fmt.Sprintf("%d day", t/3600/24)
	case t > 3600*2:
		return fmt.Sprintf("%d hour", t/3600)
	case t > 60*2:
		return fmt.Sprintf("%d min", t/60)
	default:
		return fmt.Sprintf("%d sec", t)
	}
	return ""
}

func sum(l interface{}) uint64 {
	if li, ok := l.([]uint64); ok {
		s := uint64(0)
		for _, n := range li {
			s += n
		}
		return s
	}
	return 0
}

func sizer(v interface{}) string {
	var n float32
	switch i := v.(type) {
	case uint64:
		n = float32(i)
	case int:
		n = float32(i)
	case int64:
		n = float32(i)
	case float64:
		n = float32(i)
	case float32:
		n = i
	default:
		return "0"
	}
	unit := 0
	var units = []string{"", "K", "M", "G", "T", "P"}
	for n > 1024.0 {
		n /= 1024
		unit += 1
	}
	s := fmt.Sprintf("%2.1f", n)
	if strings.HasSuffix(s, ".0") || len(s) >= 4 {
		s = s[:len(s)-2]
	}
	return s + units[unit]
}

func number(v interface{}) string {
	var n float32
	switch i := v.(type) {
	case uint64:
		n = float32(i)
	case int:
		n = float32(i)
	case int64:
		n = float32(i)
	case float64:
		n = float32(i)
	case float32:
		n = i
	default:
		return "0"
	}
	unit := 0
	var units = []string{"", "k", "m", "b"}
	for n > 1000.0 {
		n /= 1000
		unit += 1
	}
	s := fmt.Sprintf("%2.1f", n)
	if strings.HasSuffix(s, ".0") || len(s) >= 4 {
		s = s[:len(s)-2]
	}
	return s + units[unit]
}

var tmpls *template.Template
var SECTIONS = [][]string{{"IN", "Info"}, {"SS", "Server"}} //, {"ST", "Status"}}

var server_stats []map[string]interface{}
var proxy_stats []map[string]interface{}
var total_records, uniq_records uint64
var bucket_stats []string
var schd Scheduler

func update_stats(servers []string, server_stats []map[string]interface{}, isNode bool) {
	N := len(servers)
	hosts := make([]*Host, N)
	for i, s := range servers {
		hosts[i] = NewHost(s)
	}
	/*    defer func() {
	      if err := recover(); err != nil {
	          log.Print("update stats failed", err)
	      }
	  }()*/
	for {
		for i, h := range hosts {
			t, err := h.Stat(nil)
			if err != nil {
				server_stats[i] = map[string]interface{}{"name": h.Addr}
				continue
			}

			st := make(map[string]interface{})
			st["name"] = h.Addr
			//log.Print(h.Addr, t)
			for k, v := range t {
				switch k {
				case "version", "pid":
					st[k] = v
				case "rusage_maxrss":
					if n, e := strconv.ParseInt(v, 10, 64); e == nil {
						st[k] = uint64(n) * 1024
					}
				case "rusage_user", "rusage_system":
					st[k], _ = strconv.ParseFloat(v, 64)
				default:
					var e error
					st[k], e = strconv.ParseUint(v, 10, 64)
					if e != nil {
						println("conv to ui64 failed", v)
						st[k] = 0
					}
				}
			}
			stv := func(name string) uint64 {
				v, ok := st[name]
				if !ok {
					return 0
				}
				r, ok := v.(uint64)
				if ok {
					return r
				}
				return 0
			}
			st["hit"] = stv("get_hits") * 100 / (stv("cmd_get") + 1)
			st["getset"] = float32(stv("cmd_get")) / float32(stv("cmd_set")+100.0)
			st["slow"] = stv("slow_cmd") * 100 / (stv("cmd_get") + stv("cmd_set") + stv("cmd_delete") + 1)
			if maxrss, ok := st["rusage_maxrss"]; ok {
				st["mpr"] = maxrss.(uint64) / (stv("total_items") + stv("curr_items") + 1000)
			}
			old := server_stats[i]
			keys := []string{"uptime", "cmd_get", "cmd_set", "cmd_delete", "slow_cmd", "get_hits", "get_misses", "bytes_read", "bytes_written"}
			if old != nil && len(old) > 2 {
				for _, k := range keys {
					if v, ok := st[k]; ok {
						if ov, ok := old[k]; ok {
							st["curr_"+k] = v.(uint64) - ov.(uint64)
						} else {
							log.Print("no in old", k)
						}
					} else {
						log.Print("no", k)
					}
				}
			} else {
				for _, k := range keys {
					st["curr_"+k] = uint64(0)
				}
				st["curr_uptime"] = uint64(1)
			}
			st["curr_hit"] = stv("curr_get_hits") * 100 / (stv("curr_cmd_get") + 1)
			st["curr_getset"] = float32(stv("curr_cmd_get")) / float32(stv("curr_cmd_set")+1.0)
			st["curr_slow"] = stv("curr_slow_cmd") * 100 / (stv("curr_cmd_get") + stv("curr_cmd_set") + stv("curr_cmd_delete") + 1)
			keys = []string{"cmd_get", "cmd_set", "cmd_delete", "bytes_read", "bytes_written"}
			dt := float32(stv("curr_uptime"))
			for _, k := range keys {
				st["curr_"+k] = float32(stv("curr_"+k)) / dt
			}

			if isNode {
				rs, err := h.Get("@")
				if err != nil || rs == nil {
					server_stats[i] = st
					continue
				}
				bs := make([]uint64, 16)
				for i, line := range bytes.SplitN(rs.Body, []byte("\n"), 17) {
					if bytes.Count(line, []byte(" ")) < 2 || line[1] != '/' {
						continue
					}
					vv := bytes.SplitN(line, []byte(" "), 3)
					cnt, _ := strconv.ParseUint(string(vv[2]), 10, 64)
					bs[i] = cnt
				}
				st["buckets"] = bs
			}
			server_stats[i] = st
		}
		if isNode {
			total := uint64(0)
			utotal := uint64(0)
			cnt := make([]int, 16)
			m := make([]uint64, 16)
			for i := 0; i < 16; i++ {
				for _, st := range server_stats {
					if st == nil || st["buckets"] == nil {
						continue
					}
					n := st["buckets"].([]uint64)[i]
					total += n
					if n > m[i] {
						m[i] = n
					}
				}
				utotal += m[i]
				for _, st := range server_stats {
					if st == nil || st["buckets"] == nil {
						continue
					}
					n := st["buckets"].([]uint64)[i]
					if n > m[i]*98/100 {
						cnt[i] += 1
					}
				}
			}
			for i := 0; i < 16; i++ {
				switch cnt[i] {
				case 2:
					bucket_stats[i] = "warning"
				case 1:
					bucket_stats[i] = "dangerous"
				case 0:
					bucket_stats[i] = "invalid"
				}
			}
			for _, st := range server_stats {
				if st == nil || st["buckets"] == nil {
					continue
				}
				for i, c := range st["buckets"].([]uint64) {
					if c > m[i]*98/100 && cnt[i] < 3 {
						if cnt[i] == 1 {
							st["status"] = "dangerous"
						} else {
							st["status"] = "warning"
						}
						break
					}
					if c > 0 && c < m[i]-5 {
						st["status"] = "warning"
					}
				}
			}
			total_records = total
			uniq_records = utotal
		}
		time.Sleep(1e10) // 10s
	}
}

func init() {
}

func Status(w http.ResponseWriter, req *http.Request) {
	funcs := make(template.FuncMap)
	funcs["in"] = in
	funcs["sum"] = sum
	funcs["size"] = sizer
	funcs["num"] = number
	funcs["time"] = timer

	tmpls = new(template.Template)
	tmpls = tmpls.Funcs(funcs)
	tmpls = template.Must(tmpls.ParseFiles("static/index.html", "static/header.html",
		"static/info.html", "static/matrix.html", "static/server.html", "static/stats.html"))

	sections := req.FormValue("sections")
	if len(sections) == 0 {
		sections = "IN|SS"
	}
	all_sections := [][]string{}
	last := "U"
	for _, s := range SECTIONS {
		now := "U"
		if strings.Contains(sections, s[0]) {
			now = "S"
		}
		all_sections = append(all_sections, []string{last + now, s[0], s[1]})
		last = now
	}

	data := make(map[string]interface{})
	data["sections"] = sections
	data["all_sections"] = all_sections
	data["server_stats"] = server_stats
	data["proxy_stats"] = proxy_stats
	data["bucket_stats"] = bucket_stats
	data["total_records"] = total_records
	data["uniq_records"] = uniq_records

	st := schd.Stats()
	stats := make([]map[string]interface{}, len(server_stats))
	for i, _ := range stats {
		d := make(map[string]interface{})
		name := server_stats[i]["name"].(string)
		d["name"] = name
		d["stat"] = st[name]
		stats[i] = d
	}
	data["stats"] = stats

	err := tmpls.ExecuteTemplate(w, "index.html", data)
	if err != nil {
		println("render", err.Error())
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	flag.Parse()
	c, err := config.ReadDefault(*conf)
	if err != nil {
		log.Fatal("read config failed", *conf, err.Error())
	}
	if threads, e := c.Int("default", "threads"); e == nil {
		runtime.GOMAXPROCS(threads)
	}

	default_server_port, e := c.String("default", "server_port")
	if e != nil {
		default_server_port = "7900"
	}
	serverss, e := c.String("default", "servers")
	if e != nil {
		log.Fatal("no servers in conf")
	}
	servers := strings.Split(serverss, ",")
	for i := 0; i < len(servers); i++ {
		s := servers[i]
		if strings.Contains(s, "-") {
			n := len(s)
			start, _ := strconv.Atoi(s[n-3 : n-2])
			end, _ := strconv.Atoi(s[n-1:])
			for j := start + 1; j <= end; j++ {
				servers = append(servers, fmt.Sprintf("%s%d", s[:n-3], j))
			}
			s = s[:n-2]
			servers[i] = s
		}
		if !strings.Contains(s, ":") {
			servers[i] = s + ":" + default_server_port
		}
	}
	//log.Println(servers)
	sort.Strings(servers)

	if port, e := c.Int("monitor", "port"); e != nil {
		log.Print("no port in conf", e.Error())
	} else {
		server_stats = make([]map[string]interface{}, len(servers))
		bucket_stats = make([]string, 16)
		go update_stats(servers, server_stats, true)

		proxys, e := c.String("monitor", "proxy")
		if e != nil {
			proxys = fmt.Sprintf("localhost:%d", port)
		}
		proxies := strings.Split(proxys, ",")
		proxy_stats = make([]map[string]interface{}, len(proxies))
		go update_stats(proxies, proxy_stats, false)

		http.Handle("/", http.HandlerFunc(Status))
		http.Handle("/static/", http.FileServer(http.Dir("./")))
		go func() {
			listen, e := c.String("monitor", "listen")
			if e != nil {
				listen = "0.0.0.0"
			}
			addr := fmt.Sprintf("%s:%d", listen, port)
			lt, e := net.Listen("tcp", addr)
			if e != nil {
				log.Println("monitor listen failed on ", addr, e)
			}
			log.Println("monitor listen on ", addr)
			http.Serve(lt, nil)
		}()
	}

	AllocLimit = *allocLimit

	if *debug {
		AccessLog = log.New(os.Stdout, "", log.Ldate|log.Ltime)
	} else if accesslog, e := c.String("proxy", "accesslog"); e == nil {
		logf, err := os.OpenFile(accesslog, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			log.Print("open " + accesslog + " failed：" + err.Error())
		}
		AccessLog = log.New(logf, "", log.Ldate|log.Ltime)
	}
	slow, err := c.Int("proxy", "slow")
	if err != nil {
		slow = 100
	}
	SlowCmdTime = time.Duration(int64(slow) * 1e6)

	schd = NewAutoScheduler(servers, 16)
	client := NewClient(schd)
	n := len(servers)
	N, e := c.Int("proxy", "N")
	if e == nil {
		client.N = min(N, n)
	} else {
		client.N = min(n, 3)
	}
	W, e := c.Int("proxy", "W")
	if e == nil {
		client.W = min(W, n-1)
	} else {
		client.W = min(n-1, 2)
	}
	proxy := NewServer(client)
	listen, e := c.String("proxy", "listen")
	if e != nil {
		listen = "0.0.0.0"
	}
	port, e := c.Int("proxy", "port")
	if e != nil {
		log.Fatal("no proxy port in conf", e.Error())
	}
	addr := fmt.Sprintf("%s:%d", listen, port)
	if e = proxy.Listen(addr); e != nil {
		log.Fatal("proxy listen failed", e.Error())
	}

	log.Println("proxy listen on ", addr)
	proxy.Serve()
	log.Print("shut down gracefully.")
}
