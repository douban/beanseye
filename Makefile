all: fmt install

GOPATH:=$(CURDIR)
export GOPATH

fmt:
	gofmt -w -l -tabwidth=4 src/*/*.go

dep:
	go get github.com/kless/goconfig/config
	go install github.com/kless/goconfig/config
   	
install:dep fmt
	go install proxy

test:
	go test memcache
