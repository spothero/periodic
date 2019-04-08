default_target: all

all: lint test

uname := $(shell sh -c 'uname -s')
gopath := $(go env GOPATH)
ifeq ($(uname),Linux)
	LINTER_INSTALL=curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(gopath)/bin latest
endif
ifeq ($(uname),Darwin)
	LINTER_INSTALL=brew install golangci/tap/golangci-lint
endif

.PHONY: bootstrap
bootstrap:
	$(LINTER_INSTALL)

tidy:
	go mod tidy

build: tidy
	go build

test: tidy
	go test -race -v github.com/spothero/periodic -coverprofile=coverage.txt -covermode=atomic

coverage: test
	go tool cover -html=coverage.txt

clean:
	rm -rf vendor

lint:
	golangci-lint run
