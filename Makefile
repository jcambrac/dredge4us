SHELL := /bin/bash

.PHONY: lint build-api build-poller build-summarizer docker-build-api docker-build-poller docker-build-summarizer run-api run-poller run-summarizer run-frontend

lint:
	@fail=0; \
	(cd lib && golangci-lint run ./...) || fail=1; \
	(cd server && golangci-lint run ./...) || fail=1; \
	exit $$fail

build-api:
	cd server && go build -o ../bin/api ./cmd/api

build-poller:
	cd server && go build -o ../bin/poller ./cmd/poller

build-summarizer:
	cd server && go build -o ../bin/summarizer ./cmd/summarizer

# Reads DATABASE_URL (and friends) from the repo-root .env. Port matches
# what frontend/.env.local expects API_BASE_URL to be.
run-api:
	set -a && source .env && set +a && cd server && API_ADDR=:8090 go run ./cmd/api

run-poller:
	set -a && source .env && set +a && cd server && go run ./cmd/poller

# Long-running — generates immediately, then again every hour/day/week.
run-summarizer:
	set -a && source .env && set +a && cd server && go run ./cmd/summarizer

run-frontend:
	cd frontend && npm run dev

docker-build-api:
	docker build -t dredge4us-api .

docker-build-poller:
	docker build -f Dockerfile.poller -t dredge4us-poller .

docker-build-summarizer:
	docker build -f Dockerfile.summarizer -t dredge4us-summarizer .

# Deploys happen by pushing to main — see .do/app.yaml. The app itself is
# created once through the App Platform console (New App > GitHub repo >
# Dockerfile); deploy_on_push handles every push after that.
