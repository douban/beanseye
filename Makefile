all: install

GOPATH:=$(CURDIR)
export GOPATH

dep:
	go get gopkg.in/yaml.v2
	go install gopkg.in/yaml.v2

install:dep
	go install proxy

test:
	go test memcache

debug:dep
	go install proxy
