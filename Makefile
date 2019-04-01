default_target: all

all: bootstrap vendor lint test

# Bootstrapping for base golang package deps
BOOTSTRAP=\
	github.com/golang/dep/cmd/dep \
	github.com/alecthomas/gometalinter \
	github.com/jstemmer/go-junit-report

$(BOOTSTRAP):
	go get -u $@

bootstrap: $(BOOTSTRAP)
	gometalinter --install

vendor:
	dep ensure -v -vendor-only

test:
	go test -race -v github.com/spothero/periodic -coverprofile=coverage.txt -covermode=atomic 2>&1 | go-junit-report> report.xml

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
