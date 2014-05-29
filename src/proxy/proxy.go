package main

import (
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/douban/goyaml"
	"io"
	"io/ioutil"
	"log"
	"math"
	. "memcache"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"text/template"
	"time"
)

var conf *string = flag.String("conf", "conf/example.yaml", "config path")

//var debug *bool = flag.Bool("debug", false, "debug info")
var allocLimit *int = flag.Int("alloc", 1024*4, "cmem alloc limit")
var basepath = flag.String("basepath", "", "base path")

var eyeconfig Eye

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func makeGzipHandler(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			fn(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		fn(gzipResponseWriter{Writer: gz, ResponseWriter: w}, r)
	}
}

func in(s, subs interface{}) (bool, error) {
	return strings.Contains(s.(string), subs.(string)), nil
}

func timer(v interface{}) string {
	if v == nil {
		return ""
	}
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
	var n float64
	switch i := v.(type) {
	case int:
		n = float64(i)
	case uint:
		n = float64(i)
	case int64:
		n = float64(i)
	case uint64:
		n = float64(i)
	case float64:
		n = float64(i)
	case float32:
		n = float64(i)
	default:
		return "0"
	}
	if math.IsInf(n, 0) {
		return "Inf"
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
	var n float64
	switch i := v.(type) {
	case int:
		n = float64(i)
	case uint:
		n = float64(i)
	case int64:
		n = float64(i)
	case uint64:
		n = float64(i)
	case float64:
		n = float64(i)
	case float32:
		n = float64(i)
	default:
		return "0"
	}
	if math.IsInf(n, 0) {
		return "Inf"
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
var SECTIONS = [][]string{{"IN", "Info"}, {"SS", "Server"}, {"ST", "Status"}}

var server_stats []map[string]interface{}
var proxy_stats []map[string]interface{}
var total_records, uniq_records uint64
var bucket_stats []string
var schd Scheduler

func update_stats(servers []string, hosts []*Host, server_stats []map[string]interface{}, isNode bool) {
	if hosts == nil {
		hosts = make([]*Host, len(servers))
		for i, s := range servers {
			hosts[i] = NewHost(s)
		}
	}

	// call self after 10 seconds
	time.AfterFunc(time.Second*10, func() {
		update_stats(servers, hosts, server_stats, isNode)
	})

	defer func() {
		if err := recover(); err != nil {
			log.Print("update stats failed", err)
		}
	}()

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

		ST := func(name string) uint64 {
			if v, ok := st[name]; ok && v != nil {
				return v.(uint64)
			}
			return 0
		}

		st["slow_cmd"] = ST("slow_cmd")
		st["hit"] = ST("get_hits") * 100 / (ST("cmd_get") + 1)
		st["getset"] = float32(ST("cmd_get")) / float32(ST("cmd_set")+100.0)
		st["slow"] = ST("cmd_slow") * 100 / (ST("cmd_get") + ST("cmd_set") + ST("cmd_delete") + 1)
		if maxrss, ok := st["rusage_maxrss"]; ok {
			st["mpr"] = maxrss.(uint64) / (st["total_items"].(uint64) + st["curr_items"].(uint64) + 1000)
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
		st["curr_hit"] = st["curr_get_hits"].(uint64) * 100 / (st["curr_cmd_get"].(uint64) + 1)
		st["curr_getset"] = float32(st["curr_cmd_get"].(uint64)) / float32(st["curr_cmd_set"].(uint64)+1.0)
		st["curr_slow"] = st["curr_slow_cmd"].(uint64) * 100 / (st["curr_cmd_get"].(uint64) + st["curr_cmd_set"].(uint64) + st["curr_cmd_delete"].(uint64) + 1)
		keys = []string{"cmd_get", "cmd_set", "cmd_delete", "bytes_read", "bytes_written"}
		dt := float32(st["curr_uptime"].(uint64))
		for _, k := range keys {
			st["curr_"+k] = float32(st["curr_"+k].(uint64)) / dt
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
}

func Init(basepath string) {
	funcs := make(template.FuncMap)
	funcs["in"] = in
	funcs["sum"] = sum
	funcs["size"] = sizer
	funcs["num"] = number
	funcs["time"] = timer

	if !bytes.HasSuffix([]byte(basepath), []byte("/")) {
		basepath = basepath + "/"
	}

	tmpls = new(template.Template)
	tmpls = tmpls.Funcs(funcs)
	tmpls = template.Must(tmpls.ParseFiles(basepath+"static/index.html",
		basepath+"static/header.html", basepath+"static/info.html",
		basepath+"static/matrix.html", basepath+"static/server.html",
		basepath+"static/stats.html"))
}

func Status(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

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
	//c, err := config.ReadDefault(*conf)
	content, err := ioutil.ReadFile(*conf)
	if err != nil {
		log.Fatal("read config failed", *conf, err.Error())
	}

	if err := goyaml.Unmarshal(content, &eyeconfig); err != nil {
		log.Fatal("unmarshal yaml format config failed")
	}
	if *basepath == "" {
		if eyeconfig.Basepath == "" {
			curr_path, err1 := os.Getwd()
			if err1 != nil {
				log.Fatal("Cannot get pwd")
				return
			}
			*basepath = curr_path
		} else {
			*basepath = eyeconfig.Basepath
		}
	}
	Init(*basepath)

	if eyeconfig.Threads > 0 {
		runtime.GOMAXPROCS(eyeconfig.Threads)
	}

	if len(eyeconfig.Servers) == 0 {
		log.Fatal("no servers in conf")
	}
	server_configs := make(map[string][]string, len(eyeconfig.Servers))
	for _, server := range eyeconfig.Servers {
		fields := strings.Split(server, " ")
		server_configs[fields[0]] = fields[1:]
	}
	servers := make([]string, 0, len(server_configs))
	for server, _ := range server_configs {
		servers = append(servers, server)
	}

	if eyeconfig.WebPort <= 0 {
		log.Print("error webport in conf: ", eyeconfig.WebPort)
	} else if eyeconfig.Buckets <= 0 {
		log.Print("error buckets in conf: ", eyeconfig.Buckets)
	} else {
		server_stats = make([]map[string]interface{}, len(servers))
		bucket_stats = make([]string, eyeconfig.Buckets)
		go update_stats(servers, nil, server_stats, true)

		if len(eyeconfig.Proxies) > 0 {
			proxy_stats = make([]map[string]interface{}, len(eyeconfig.Proxies))
			go update_stats(eyeconfig.Proxies, nil, proxy_stats, false)
		}

		http.Handle("/", http.HandlerFunc(makeGzipHandler(Status)))
		http.Handle("/static/", http.FileServer(http.Dir(*basepath)))
		go func() {
			if len(eyeconfig.Listen) == 0 {
				eyeconfig.Listen = "0.0.0.0"
			}
			addr := fmt.Sprintf("%s:%d", eyeconfig.Listen, eyeconfig.WebPort)
			lt, e := net.Listen("tcp", addr)
			if e != nil {
				log.Println("monitor listen failed on ", addr, e)
			}
			log.Println("monitor listen on ", addr)
			http.Serve(lt, nil)
		}()
	}

	AllocLimit = *allocLimit

    var success bool

	if len(eyeconfig.AccessLog) > 0 {
        AccessLogPath = eyeconfig.AccessLog
        if success, err = OpenAccessLog(eyeconfig.AccessLog); !success {
            log.Fatalf("open AccessLog file in path: %s with error : %s", eyeconfig.AccessLog, err.Error())
        }
	}

	if len(eyeconfig.ErrorLog) > 0 {
        ErrorLogPath = eyeconfig.ErrorLog
        if success, err = OpenErrorLog(eyeconfig.ErrorLog); !success {
            log.Fatalf("open ErrorLog file in path: %s with error : %s", eyeconfig.ErrorLog, err.Error())
        }
	}

	slow := eyeconfig.Slow
	if slow == 0 {
		slow = 100
	}
	SlowCmdTime = time.Duration(int64(slow) * 1e6)

	readonly := eyeconfig.Readonly

	n := len(servers)
	if eyeconfig.N == 0 {
		eyeconfig.N = 3
	}
	N := min(eyeconfig.N, n)

	if eyeconfig.W == 0 {
		eyeconfig.W = 2
	}
	W := min(eyeconfig.W, n-1)

	if eyeconfig.R == 0 {
		eyeconfig.R = 1
	}
	R := eyeconfig.R

	//schd = NewAutoScheduler(servers, 16)
	schd = NewManualScheduler(server_configs, eyeconfig.Buckets, N)

	var client DistributeStorage
	if readonly {
		client = NewRClient(schd, N, W, R)
	} else {
		client = NewClient(schd, N, W, R)
	}

	http.HandleFunc("/data", func(w http.ResponseWriter, req *http.Request) {
	})

	proxy := NewServer(client)
	if eyeconfig.Port <= 0 {
		log.Fatal("error proxy port in config it is ", eyeconfig.Port)
	}
	addr := fmt.Sprintf("%s:%d", eyeconfig.Listen, eyeconfig.Port)
	if e := proxy.Listen(addr); e != nil {
		log.Fatal("proxy listen failed", e.Error())
	}

	log.Println("proxy listen on ", addr)
	proxy.Serve()
	log.Print("shut down gracefully.")
}
