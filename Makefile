DATABASE_URL ?= postgres://earendil:earendil@localhost:5432/earendil?sslmode=disable
export DATABASE_URL

export GOOSE_DRIVER 	   := postgres
export GOOSE_DBSTRING 	   := $(DATABASE_URL)
export GOOSE_MIGRATION_DIR := migrations

.PHONY: db-up db-down db-reset db-psql migrate-up migrate-down migrate-status migrate-new sqlc sqlc-check seed test vet run

db-up:
	docker compose up -d --wait

db-down:
	docker compose down

db-reset:
	docker compose down -v
	docker compose up -d --wait

db-psql:
	docker compose exec db psql -U earendil -d earendil

migrate-up:
	go tool goose up

migrate-down:
	go tool goose down

migrate-status:
	go tool goose status

migrate-new:
	@test -n "$(name)" || (echo "usage: make migrate-new name=add_something" && exit 1)
	go tool goose -s create $(name) sql

sqlc:
	go tool sqlc generate

sqlc-check:
	go tool sqlc diff

seed:
	docker compose exec -T db psql -v ON_ERROR_STOP=1 -U earendil -d earendil < dev/seed.sql

test:
	go test -race ./...

vet:
	go vet ./...

run:
	go run ./cmd/earendil
