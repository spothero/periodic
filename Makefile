default_target: all

all: bootstrap vendor test

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

clean:
	rm -rf vendor
