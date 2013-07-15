all: fmt install

GOPATH:=$(CURDIR)
export GOPATH

fmt:
	gofmt -w -l -tabwidth=4 src/*/*.go

dep:
	go get github.com/robfig/config
	go install github.com/robfig/config
   	
install:dep fmt
	go install proxy

test:
	go test memcache
