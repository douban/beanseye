all: install

GOPATH:=$(CURDIR)
export GOPATH

dep:
	go get github.com/hurricane1026/goyaml
	go install github.com/hurricane1026/goyaml
	go get github.com/hurricane1026/go-bit/bit

install:dep
	go install proxy

test:
	go test memcache

debug:dep
	go install proxy
