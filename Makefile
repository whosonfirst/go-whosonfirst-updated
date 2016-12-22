CWD=$(shell pwd)
GOPATH := $(CWD)

prep:
	if test -d pkg; then rm -rf pkg; fi

self:   prep
	if test ! -d src; then mkdir src; fi
	if test ! -d src/github.com/whosonfirst/go-whosonfirst-updated/updated; then mkdir -p src/github.com/whosonfirst/go-whosonfirst-updated/; fi
	cp  updated.go src/github.com/whosonfirst/go-whosonfirst-updated/
	cp -r process src/github.com/whosonfirst/go-whosonfirst-updated/
	cp -r queue src/github.com/whosonfirst/go-whosonfirst-updated/
	cp -r vendor/src/* src/

rmdeps:
	if test -d src; then rm -rf src; fi 

build:	fmt bin

deps:   rmdeps
	@GOPATH=$(GOPATH) go get -u "github.com/whosonfirst/go-whosonfirst-s3"
	@GOPATH=$(GOPATH) go get -u "github.com/whosonfirst/go-whosonfirst-log"
	# @GOPATH=$(GOPATH) go get -u "github.com/whosonfirst/go-whosonfirst-tile38/index"
	@GOPATH=$(GOPATH) go get -u "gopkg.in/redis.v1"

vendor-deps: deps
	if test ! -d vendor; then mkdir vendor; fi
	if test -d vendor/src; then rm -rf vendor/src; fi
	cp -r src vendor/src
	find vendor -name '.git' -print -type d -exec rm -rf {} +

bin: 	self
	@GOPATH=$(GOPATH) go build -o bin/wof-updated cmd/wof-updated.go

fmt:
	go fmt cmd/*.go
	go fmt process/*.go
	go fmt queue/*.go
	go fmt updated.go
