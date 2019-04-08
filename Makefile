default_target: all

all: lint test

uname := $(shell sh -c 'uname -s')
ifeq ($(uname),Linux)
	LINTER_INSTALL=curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s v1.16.0
endif
ifeq ($(uname),Darwin)
	LINTER_INSTALL=brew install golangci/tap/golangci-lint
endif

.PHONY: bootstrap
bootstrap:
	$(LINTER_INSTALL)

install:
	go install

build: install
	go build

test: install
	go test -race -v github.com/spothero/periodic -coverprofile=coverage.txt -covermode=atomic

coverage: test
	go tool cover -html=coverage.txt

clean:
	rm -rf vendor

lint:
	golangci-lint run
