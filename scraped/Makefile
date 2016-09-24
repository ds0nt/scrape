

all: regenerate build

regenerate:
	protoc -I ./service ./service/service.proto --go_out=plugins=grpc:service

build:
	go build .

.PHONY: all build regenerate
