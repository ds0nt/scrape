

all: regenerate build

regenerate:
	protoc -I ./service ./service/service.proto --go_out=plugins=grpc:service

build:
	go build .

sandbox: regenerate build
	./scraped -data-dir sandbox

.PHONY: all build regenerate
