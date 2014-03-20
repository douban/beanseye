package memcache

import (
    "bytes"
    "fmt"
    "github.com/hurricane1026/go-bit/bit"
    "math"
    "sort"
    "strconv"
    "strings"
    "time"
    "math/rand"
)

// Scheduler: route request to nodes
type Scheduler interface {
    Feedback(host *Host, key string, adjust float64) // feedback for auto routing
    GetHostsByKey(key string) []*Host                               // route a key to hosts
    DivideKeysByBucket(keys []string) [][]string                    // route some keys to group of hosts
    Stats() map[string][]float64                                    // internal status
}

type emptyScheduler struct{}

func (c emptyScheduler) Feedback(host *Host, key string, adjust float64) {}

func (c emptyScheduler) Stats() map[string][]float64 { return nil }

// route request by Mod of HASH
type ModScheduler struct {
    hosts      []*Host
    hashMethod HashMethod
    emptyScheduler
}

func NewModScheduler(hosts []string, hashname string) Scheduler {
    var c ModScheduler
    c.hosts = make([]*Host, len(hosts))
    c.hashMethod = hashMethods[hashname]
    for i, h := range hosts {
        c.hosts[i] = NewHost(h)
    }
    return &c
}

func (c *ModScheduler) GetHostsByKey(key string) []*Host {
    h := c.hashMethod([]byte(key))
    r := make([]*Host, 1)
    r[0] = c.hosts[h%uint32(len(c.hosts))]
    return r
}

func (c *ModScheduler) DivideKeysByBucket(keys []string) [][]string {
    n := len(c.hosts)
    rs := make([][]string, n)
    for _, key := range keys {
        h := c.hashMethod([]byte(key)) % uint32(n)
        rs[h] = append(rs[h], key)
    }
    return rs
}

// internal status
func (c *ModScheduler) Stats() map[string][]float64 {
    r := make(map[string][]float64)
    for i, h := range c.hosts {
        r[h.Addr] = make([]float64, len(c.hosts))
        r[h.Addr][i] = 1
    }
    return r
}

type uint64Slice []uint64

func (l uint64Slice) Len() int {
    return len(l)
}

func (l uint64Slice) Less(i, j int) bool {
    return l[i] < l[j]
}

func (l uint64Slice) Swap(i, j int) {
    l[i], l[j] = l[j], l[i]
}

// route requests by consistant hash
type ConsistantHashScheduler struct {
    hosts      []*Host
    index      []uint64
    hashMethod HashMethod
    emptyScheduler
}

const VIRTUAL_NODES = 100

func NewConsistantHashScheduler(hosts []string, hashname string) Scheduler {
    var c ConsistantHashScheduler
    c.hosts = make([]*Host, len(hosts))
    c.index = make([]uint64, len(hosts)*VIRTUAL_NODES)
    c.hashMethod = hashMethods[hashname]
    for i, h := range hosts {
        c.hosts[i] = NewHost(h)
        for j := 0; j < VIRTUAL_NODES; j++ {
            v := c.hashMethod([]byte(fmt.Sprintf("%s-%d", h, j)))
            ps := strings.SplitN(h, ":", 2)
            host := ps[0]
            port := ps[1]
            if port == "11211" {
                v = c.hashMethod([]byte(fmt.Sprintf("%s-%d", host, j)))
            }
            c.index[i*VIRTUAL_NODES+j] = (uint64(v) << 32) + uint64(i)
        }
    }
    sort.Sort(uint64Slice(c.index))
    if !sort.IsSorted(uint64Slice(c.index)) {
        panic("sort failed")
    }
    return &c
}

func (c *ConsistantHashScheduler) getHostIndex(key string) int {
    h := uint64(c.hashMethod([]byte(key))) << 32
    N := len(c.index)
    i := sort.Search(N, func(k int) bool { return c.index[k] >= h })
    if i == N {
        i = 0
    }
    return int(c.index[i] & 0xffffffff)
}

func (c *ConsistantHashScheduler) GetHostsByKey(key string) []*Host {
    r := make([]*Host, 1)
    i := c.getHostIndex(key)
    r[0] = c.hosts[i]
    return r
}

func (c *ConsistantHashScheduler) DivideKeysByBucket(keys []string) [][]string {
    n := len(c.hosts)
    rs := make([][]string, n)
    for _, key := range keys {
        i := c.getHostIndex(key)
        rs[i] = append(rs[i], key)
    }
    return rs
}

// route request by configure by hand
type ManualScheduler struct {
    N          int
    hosts      []*Host
    buckets    [][]int
    bucketWidth int
    stats      [][]float64
    hashMethod HashMethod
    feedChan   chan *Feedback
    main_nodes []*bit.Set
}

// the string is a Hex int string, if it start with -, it means serve the bucket as a backup
func NewManualScheduler(config map[string][]string, bs, n int) *ManualScheduler {
    defer func() {
        if r := recover(); r != nil {
            ErrorLog.Fatalln("NewManualScheduler panic, maybe node's supporting bucket more than buckets number")
        }
    }()
    c := new(ManualScheduler)
    c.hosts = make([]*Host, len(config))
    c.buckets = make([][]int, bs)
    c.stats = make([][]float64, bs)
    c.N = n

    no := 0
    for addr, serve_to := range config {
        host := NewHost(addr)
        host.offset = no
        c.hosts[no] = host
        for _, bucket_str := range serve_to {
            if strings.HasPrefix(bucket_str, "-") {
                if bucket, e := strconv.ParseInt(bucket_str[1:], 16, 16); e == nil {
                    c.buckets[bucket] = append(c.buckets[bucket], no)
                } else {
                    ErrorLog.Println("Parse serving bucket config failed, it was not digital")
                }
            } else {
                if bucket, e := strconv.ParseInt(bucket_str, 16, 16); e == nil {
                    c.buckets[bucket] = append([]int{no}, c.buckets[bucket]...)
                } else {
                    ErrorLog.Println("Parse serving bucket config failed, it was not digital")
                }
            }
        }
        no++
    }

    // set c.stats according to c.buckets
    /*
    for b := 0; b < bs; b++ {
        c.stats[b] = make([]float64, len(c.hosts))
        // set main server's stat to be 10.0
    }
    */
    for i, bucket := range c.buckets {
        c.stats[i] = make([]float64, len(c.hosts))
        // set main server's stat to be 10.0
        for _, main_node_offset := range bucket[:c.N] {
            c.stats[i][main_node_offset] = 10.0
        }
    }

    // record the main nodes in main_buckets
    c.main_nodes = make([]*bit.Set, bs)
    for i, bucket := range c.buckets {
        c.main_nodes[i] = bit.New(bucket[:c.N]...)
    }

    c.hashMethod = fnv1a1
    c.bucketWidth = calBitWidth(bs)

    go c.procFeedback()
    go func() {
        for {
            c.try_recovery()
            time.Sleep(10 * 1e9)
        }
    }()
    return c
}

func fastdivideKeysByBucket(hash_func HashMethod, bs int, bw int, keys []string) [][]string {
    rs := make([][]string, bs)
    //bw := calBitWidth(bs)
    for _, key := range keys {
        b := getBucketByKey(hash_func, bw, key)
        rs[b] = append(rs[b], key)
    }
    return rs
}


/*
func (c *ManualScheduler) dump_scores() {
    for i, bucket := range c.buckets {
        scores := make([]string, len(bucket))
        stats := c.stats[i]
        for j, n := range bucket {
            addr := c.hosts[n].Addr
            scores[j] = fmt.Sprintf("%s:%f", addr[:strings.Index(addr, ":")], stats[n])
        }
        ErrorLog.Printf( "Bucket %X Score: %v", i, scores)
    }
}
*/

func (c *ManualScheduler) try_recovery() {
    //c.dump_scores()
    for i, bucket := range c.buckets {
        curr := bit.New(bucket[:c.N]...)
        down_node := c.main_nodes[i].AndNot(curr)
        if down_node.IsEmpty() {
            // no down nodes, just skip
            // random raward 2nd, 3rd node
            second_reward := float64(rand.Intn(16))
            third_reward := float64(rand.Intn(16))
            c.feedChan <- &Feedback {hostIndex: bucket[1], bucketIndex: i, adjust: second_reward}
            c.feedChan <- &Feedback {hostIndex: bucket[2], bucketIndex: i, adjust: third_reward}
        } else {
            addrs := make([]string, len(bucket))
            for j, node := range bucket {
                addr := c.hosts[node].Addr
                addrs[j] = addr[:strings.Index(addr, ":")]
            }
            bucket_content := strings.Join(addrs, ", ")
            ErrorLog.Printf("DownBucket %X: [%s]", i, bucket_content)
            downs := down_node.Slice()
            down_addrs := make([]string, len(downs))
            for j, h := range downs {
                addr := c.hosts[h].Addr
                down_addrs[j] = addr[:strings.Index(addr, ":")]
            }
            down_content := strings.Join(down_addrs, ", ")
            ErrorLog.Printf("Down MainNodes: %s", down_content)
            recovered := 0
            for _, node := range down_node.Slice() {
                host := c.hosts[node]
                if _, err := host.Get("@"); err == nil {
                    // no err now, swap to main portion
                    c.feedChan <- &Feedback{hostIndex: node, bucketIndex: i, adjust: 30}
                    recovered++
                }
            }
            // calculate a backup node in the first N
            backup_node := curr.AndNot(c.main_nodes[i])
            for _, node := range backup_node.Slice() {
                if recovered > 0 {
                    ErrorLog.Printf("Backup node : %s get punishment -30", c.hosts[node].Addr)
                    c.feedChan <- &Feedback{hostIndex: node, bucketIndex: i, adjust: -30}
                    recovered--
                } else {
                    break
                }
            }
        }
    }
}

func (c *ManualScheduler) procFeedback() {
    c.feedChan = make(chan *Feedback, 256)
    for {
        fb := <-c.feedChan
        c.feedback(fb.hostIndex, fb.bucketIndex, fb.adjust)
    }
}

func (c *ManualScheduler) feedback(i, index int, adjust float64) {
    stats := c.stats[index]
    old := stats[i]
    stats[i] += adjust
    if stats[i] < 0 && old > 0 && adjust > -4.9 {
        stats[i] = 0
    }

    // try to reduce the bucket's stats
    if stats[i] > 80 {
        for index := 0; index < len(stats); index++ {
            stats[index] = stats[index] / 1.5
        }
    }
    bucket_len := len(c.buckets[index])
    bucket := make([]int, bucket_len)
    copy(bucket, c.buckets[index])

    k := 0
    // find the position
    for k = 0; k < bucket_len; k++ {
        if bucket[k] == i {
            break
        }
    }

    if stats[i]-old > 0 {
        for k > 0 && stats[bucket[k]] > stats[bucket[k-1]] {
            swap(bucket, k, k-1)
            k--
        }
    } else {
        for k < bucket_len -1 && stats[bucket[k]] < stats[bucket[k+1]] {
            swap(bucket, k, k+1)
            k++
        }
    }
    // set it to origin
    c.buckets[index] = bucket
}

func (c *ManualScheduler) GetHostsByKey(key string) (host []*Host) {
    i := getBucketByKey(c.hashMethod, c.bucketWidth, key)
    host = make([]*Host, len(c.buckets[i]))
    for j, addr := range c.buckets[i] {
        host[j] = c.hosts[addr]
    }
    return
}

func (c *ManualScheduler) DivideKeysByBucket(keys []string) [][]string {
    return fastdivideKeysByBucket(c.hashMethod, len(c.buckets), c.bucketWidth, keys)
}

func (c *ManualScheduler) Feedback(host *Host, key string, adjust float64) {
    index := getBucketByKey(c.hashMethod, c.bucketWidth, key)
    c.feedChan <- &Feedback{hostIndex: host.offset, bucketIndex: index, adjust: adjust}
}

func (c *ManualScheduler) Stats() map[string][]float64 {
    r := make(map[string][]float64, len(c.hosts))
    for _, h := range c.hosts {
        r[h.Addr] = make([]float64, len(c.buckets))
    }
    for i, st := range c.stats {
        for j, w := range st {
            r[c.hosts[j].Addr][i] = w
        }
    }
    return r
}

type Feedback struct {
    hostIndex   int
    bucketIndex int
    adjust      float64
}

// route requests by auto discoved infomation, used in beansdb
type AutoScheduler struct {
    n          int
    hosts      []*Host
    buckets    [][]int
    stats      [][]float64
    last_check time.Time
    hashMethod HashMethod
    feedChan   chan *Feedback
    bucketWidth int
}

func NewAutoScheduler(config []string, bs int) *AutoScheduler {
    c := new(AutoScheduler)
    c.n = len(config)
    c.hosts = make([]*Host, c.n)
    c.buckets = make([][]int, bs)
    c.stats = make([][]float64, bs)
    for i := 0; i < bs; i++ {
        c.buckets[i] = make([]int, c.n)
        c.stats[i] = make([]float64, c.n)
    }
    for i, addr := range config {
        c.hosts[i] = NewHost(addr)
        for j := 0; j < bs; j++ {
            c.buckets[j][i] = i
            c.stats[j][i] = 0
        }
    }
    c.hashMethod = fnv1a1
    c.bucketWidth = calBitWidth(c.n)
    go c.procFeedback()

    c.check()
    go func() {
        for {
            c.check()
            time.Sleep(10 * 1e9)
        }
    }()
    return c
}

func calBitWidth(number int) int {
    width := 0
    for number > 1 {
        width++
        number /= 2
    }
    return width
}

func getBucketByKey(hash_func HashMethod, bucketWidth int, key string) int {
    if len(key) > bucketWidth/4 && key[0] == '@' {
        return hextoi(key[1 : bucketWidth/4+1])
    }
    if len(key) >= 1 && key[0] == '?' {
        key = key[1:]
    }
    h := hash_func([]byte(key))
    return (int)(h >> (uint)(32-bucketWidth))
}

func (c *AutoScheduler) GetHostsByKey(key string) []*Host {
    i := getBucketByKey(c.hashMethod, c.bucketWidth, key)
    //host_ids := c.GetBucketSnapshot(i)
    host_ids := c.buckets[i]
    cnt := len(host_ids)
    hosts := make([]*Host, cnt)
    for j := 0; j < cnt; j++ {
        hosts[j] = c.hosts[host_ids[j]]
    }
    return hosts
}

func divideKeysByBucket(hash_func HashMethod, bs int, keys []string) [][]string {
    rs := make([][]string, bs)
    bw := calBitWidth(bs)
    for _, key := range keys {
        b := getBucketByKey(hash_func, bw, key)
        rs[b] = append(rs[b], key)
    }
    return rs
}


func (c *AutoScheduler) DivideKeysByBucket(keys []string) [][]string {
    return divideKeysByBucket(c.hashMethod, len(c.buckets), keys)
}

func (c *AutoScheduler) Stats() map[string][]float64 {
    r := make(map[string][]float64)
    for _, h := range c.hosts {
        r[h.Addr] = make([]float64, len(c.buckets))
    }
    for i, st := range c.stats {
        for j, w := range st {
            r[c.hosts[j].Addr][i] = w
        }
    }
    return r
}

func swap(a []int, j, k int) {
    a[j], a[k] = a[k], a[j]
}

func abs(x float64) float64 {
    if x < 0 {
        return -x
    }
    return x
}

func (c *AutoScheduler) hostIndex(host *Host) int {
    for i, h := range c.hosts {
        if h == host {
            return i
        }
    }
    return -1
}

func (c *AutoScheduler) procFeedback() {
    c.feedChan = make(chan *Feedback, 1024)
    for {
        fb := <-c.feedChan
        c.feedback(fb.hostIndex, fb.bucketIndex, fb.adjust)
    }
}

func (c *AutoScheduler) Feedback(host *Host, key string, adjust float64) {
    index := getBucketByKey(c.hashMethod, c.bucketWidth, key)
    i := c.hostIndex(host)
    if i < 0 {
        return
    }
    //c.feedback(i, index, adjust)
    c.feedChan <- &Feedback{hostIndex: i, bucketIndex: index, adjust: adjust}
}

func (c *AutoScheduler) feedback(i, index int, adjust float64) {
    stats := c.stats[index]
    old := stats[i]
    if adjust >= 0 {
        //log.Print("reset ", index, " ", c.hosts[i].Addr, " ", stats[i], adjust)
        stats[i] = (stats[i] + adjust) / 2
    } else {
        stats[i] += adjust
    }
    buckets := make([]int, len(c.hosts))
    copy(buckets, c.buckets[index])
    k := 0
    for k = 0; k < len(c.hosts); k++ {
        if buckets[k] == i {
            break
        }
    }
    if stats[i]-old > 0 {
        for k > 0 && stats[buckets[k]] > stats[buckets[k-1]] {
            swap(buckets, k, k-1)
            k--
        }
    } else {
        for k < len(c.hosts)-1 && stats[buckets[k]] < stats[buckets[k+1]] {
            swap(buckets, k, k+1)
            k++
        }
    }
    // set it to origin
    c.buckets[index] = buckets
}

func hextoi(hex string) int {
    r := rune(0)
    for _, c := range hex {
        r *= 16
        switch {
        case c >= '0' && c <= '9':
            r += c - '0'
        case c >= 'A' && c <= 'F':
            r += 10 + c - 'A'
        case c >= 'a' && c <= 'f':
            r += 10 + c - 'a'
        }
    }
    return int(r)
}

func (c *AutoScheduler) listHost(host *Host, dir string) {
    rs, err := host.Get(dir)
    if err != nil || rs == nil {
        return
    }
    for _, line := range bytes.SplitN(rs.Body, []byte("\n"), 17) {
        if bytes.Count(line, []byte(" ")) < 2 || line[1] != '/' {
            continue
        }
        vv := bytes.SplitN(line, []byte(" "), 3)
        cnt, _ := strconv.ParseFloat(string(vv[2]), 64)
        adjust := float64(math.Sqrt(cnt))
        c.Feedback(host, dir+string(vv[0]), adjust)
    }
}

func (c *AutoScheduler) check() {
    defer func() {
        if e := recover(); e != nil {
            ErrorLog.Print("error while check()", e)
        }
    }()
    bs := len(c.buckets)
    bucketWidth := 0
    for bs > 1 {
        bucketWidth++
        bs /= 2
    }
    count := 1 << (uint)(bucketWidth-4)
    w := bucketWidth/4 - 1
    format := fmt.Sprintf("@%%0%dx", w)
    for _, host := range c.hosts {
        if w < 1 {
            c.listHost(host, "@")
        } else {
            for i := 0; i < count; i++ {
                key := fmt.Sprintf(format, i)
                c.listHost(host, key)
            }
        }
    }

    c.last_check = time.Now()
}
