default_target: all

all: lint test

.PHONY: all tidy build test coverage lint

LINTER_INSTALLED := $(shell sh -c 'which golangci-lint')

build:
	go build

test:
	go test -race -v github.com/spothero/periodic -coverprofile=coverage.txt -covermode=atomic

coverage: test
	go tool cover -html=coverage.txt

lint:
ifdef LINTER_INSTALLED
	golangci-lint run
else
	$(error golangci-lint not found, skipping linting. Installation instructions: https://github.com/golangci/golangci-lint#ci-installation)
endif
