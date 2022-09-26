# Go parameters
GOCMD=GO111MODULE=on go
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get -v
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt

# Build time variables
ifeq ($(GIT_SHA),)
GIT_SHA:=$(shell git rev-parse HEAD)
endif

ifeq ($(GIT_DIRTY),)
GIT_DIRTY:=$(shell git diff --no-ext-diff 2> /dev/null | wc -l)
endif

.PHONY: all

all: get document-benchmark
fmt:
	$(GOFMT) ./...


checkfmt:
	@echo 'Checking gofmt';\
 	bash -c "diff -u <(echo -n) <(gofmt -d .)";\
	EXIT_CODE=$$?;\
	if [ "$$EXIT_CODE"  -ne 0 ]; then \
		echo '$@: Go files must be formatted with gofmt'; \
	fi && \
	exit $$EXIT_CODE

get:
	$(GOGET) -t -v ./...

test: get fmt
	$(GOTEST) -v -race -coverprofile=coverage.txt -covermode=atomic ./...

document-benchmark: $(wildcard ./index/*.go) $(wildcard ./ingest/*.go) $(wildcard ./query/*.go) $(wildcard ./synth/*.go) fmt
	$(GOBUILD) -o ./bin/$@ -ldflags="-X 'main.GitSHA1=$(GIT_SHA)' -X 'main.GitDirty=$(GIT_DIRTY)'" .
