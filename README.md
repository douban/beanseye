# What is beanseye ?

Beanseye is proxy and monitor for beansdb, written in Go.

# How to build

Install Go weekly release first, then 

$ export GOPATH=$PWD
$ go install proxy

# How to run 

prepare your configuration, according to conf/example.ini

$ ./bin/proxy -conf=example.ini

# Proxy

You can access whole beansdb cluster throught localhost:7905
as configured, by any memcached client.

# Monitor

There is a web monitor on http://localhost:7908/ at default.
