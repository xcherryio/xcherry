.PHONY: build test tidy docker-build compose-up compose-down

build:
	GOWORK=off go build -o bin/dex-server-all-in-one ./cmd/dex-server-all-in-one
	GOWORK=off go build -o bin/temporal-sql-tool ./cmd/temporal-sql-tool

test:
	GOWORK=off go test ./...

tidy:
	GOWORK=off go mod tidy

docker-build:
	docker build -f server/all-in-one/Dockerfile -t superdurable/dex-server-all-in-one:latest ../..

compose-up:
	docker compose up -d

compose-down:
	docker compose down
