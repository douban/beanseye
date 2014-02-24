package main

type Eye struct {
	Servers   []string
	Port      int
	WebPort   int
	Threads   int
	N         int
	W         int
	R         int
	Buckets   int
	Slow      int
	Listen    string
	Proxies   []string
	AccessLog string
	ErrorLog  string
	Basepath  string
	Readonly  bool
}
