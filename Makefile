default_target: all

all: bootstrap vendor lint test

# Bootstrapping for base golang package deps
BOOTSTRAP=\
	github.com/golang/dep/cmd/dep \
	github.com/alecthomas/gometalinter

$(BOOTSTRAP):
	go get -u $@

bootstrap: $(BOOTSTRAP)
	gometalinter --install

vendor:
	dep ensure -v -vendor-only

test:
	go test -race -v ./... -coverprofile=coverage.txt -covermode=atomic

coverage: test
	go tool cover -html=coverage.txt

clean:
	rm -rf vendor

# Linting
LINTERS=gofmt golint staticcheck vet misspell ineffassign deadcode
METALINT=gometalinter --tests --disable-all --vendor --deadline=5m -e "zz_.*\.go" ./...

lint:
	$(METALINT) $(addprefix --enable=,$(LINTERS))

$(LINTERS):
	$(METALINT) --enable=$@
