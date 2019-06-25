# Run unit tests.
test:
	go test -v ./...

# Run integration tests.
integration_test:
	go test -v -tags integration ./...
