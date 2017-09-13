CWD=$(shell pwd)
GOPATH := $(CWD)

prep:
	if test -d pkg; then rm -rf pkg; fi

self:   prep rmdeps
	if test -d src; then rm -rf src; fi
	if test ! -d src/github.com/whosonfirst/go-whosonfirst-updated/updated; then mkdir -p src/github.com/whosonfirst/go-whosonfirst-updated/; fi
	cp  updated.go src/github.com/whosonfirst/go-whosonfirst-updated/
	cp -r process src/github.com/whosonfirst/go-whosonfirst-updated/
	cp -r queue src/github.com/whosonfirst/go-whosonfirst-updated/
	cp -r utils src/github.com/whosonfirst/go-whosonfirst-updated/
	cp -r vendor/* src/

rmdeps:
	if test -d src; then rm -rf src; fi 

build:	fmt bin

deps:   rmdeps
	@GOPATH=$(GOPATH) go get -u "github.com/whosonfirst/go-whosonfirst-csv"
	@GOPATH=$(GOPATH) go get -u "github.com/whosonfirst/go-whosonfirst-s3"
	@GOPATH=$(GOPATH) go get -u "github.com/whosonfirst/go-whosonfirst-geojson-v2"
	@GOPATH=$(GOPATH) go get -u "github.com/whosonfirst/go-whosonfirst-log"
	@GOPATH=$(GOPATH) go get -u "github.com/whosonfirst/go-whosonfirst-index"
	@GOPATH=$(GOPATH) go get -u "github.com/whosonfirst/go-whosonfirst-tile38"
	@GOPATH=$(GOPATH) go get -u "github.com/whosonfirst/go-whosonfirst-uri"
	@GOPATH=$(GOPATH) go get -u "github.com/whosonfirst/go-slackcat-writer"
	@GOPATH=$(GOPATH) go get -u "gopkg.in/redis.v1"

vendor-deps: deps
	if test -d vendor; then rm -rf vendor; fi
	cp -r src vendor
	find vendor -name '.git' -print -type d -exec rm -rf {} +
	rm -rf vendor/github.com/whosonfirst/go-whosonfirst-tile38/vendor/github.com/whosonfirst/go-whosonfirst-geojson-v2

bin: 	self
	@GOPATH=$(GOPATH) go build -o bin/wof-updated cmd/wof-updated.go
	@GOPATH=$(GOPATH) go build -o bin/wof-updated-replay cmd/wof-updated-replay.go

fmt:
	go fmt cmd/*.go
	go fmt process/*.go
	go fmt queue/*.go
	go fmt utils/*.go
	go fmt updated.go
