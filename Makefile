deps:
	dep version || (curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh)
	dep ensure -v

build_deps: deps

# Run unit tests.
test: build_deps
	go test -v ./...

# Build the tessellate cli.
cli_build: build_deps
	env GOOS=linux GARCH=amd64 CGO_ENABLED=0 GOCACHE=/tmp/gocache go build -o tskv -a -installsuffix \
		cgo github.com/tsocial/tskv

cli_build_mac: build_deps
	env GOOS=darwin GARCH=amd64 CGO_ENABLED=0 GOCACHE=/tmp/gocache go build -o tskv -a -installsuffix \
		cgo github.com/tsocial/tskv
