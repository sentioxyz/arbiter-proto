# Generator versions are pinned here; `make tools` is the single source of
# truth for local dev and CI (the drift check depends on identical versions).
PROTOC_GEN_GO_VERSION      := v1.36.11
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1

.PHONY: tools proto lint breaking test tidy

tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

proto:
	buf generate

lint:
	buf lint

breaking:
	buf breaking --against '.git#branch=main'

test:
	go build ./...
	go vet ./...
	go test ./...

tidy:
	go mod tidy
