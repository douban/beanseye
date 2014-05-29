all: install

GOPATH:=$(CURDIR)
export GOPATH

dep:
	go get github.com/douban/goyaml
	go install github.com/douban/goyaml

install:dep
	go install proxy

test:
	go test memcache

debug:dep
	go install proxy
