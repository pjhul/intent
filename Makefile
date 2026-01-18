.PHONY: all build build-inserter build-all test test-coverage test-coverage-html clean docker docker-inserter flink-build flink-docker up down logs logs-inserter flink-run flink-cancel inserter-restart generate-mocks

# Variables
APP_NAME := cohort-service
VERSION ?= latest

all: build

# Go targets
build:
	go build -o bin/$(APP_NAME) ./cmd/cohort-service

build-inserter:
	go build -o bin/inserter-service ./cmd/inserter-service

build-all: build build-inserter

test:
	go test -v ./...

test-coverage:
	go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out

test-coverage-html: test-coverage
	go tool cover -html=coverage.out -o coverage.html

generate-mocks:
	go install go.uber.org/mock/mockgen@v0.5.0
	mockgen -source=internal/db/querier.go -destination=internal/mocks/mock_db.go -package=mocks
	mockgen -source=internal/domain/cohort/recompute_worker.go -destination=internal/mocks/mock_clickhouse.go -package=mocks
	mockgen -source=internal/domain/cohort/service.go -destination=internal/mocks/mock_producer.go -package=mocks

clean:
	rm -rf bin/
	rm -f $(APP_NAME)

# Generate sqlc
generate:
	sqlc generate

# Docker targets
docker:
	docker build -t $(APP_NAME):$(VERSION) .

docker-inserter:
	docker build -f Dockerfile.inserter -t inserter-service:$(VERSION) .

# Flink job
flink-build:
	cd flink && mvn clean package -DskipTests

flink-docker: flink-build
	docker build -f deploy/flink/Dockerfile -t cohort-flink:$(VERSION) .

# Docker Compose targets
up: flink-docker docker docker-inserter
	docker compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 10
	@echo "Services started. Flink UI: http://localhost:8081"

down:
	docker compose down

# View logs
logs:
	docker compose logs -f

logs-flink:
	docker compose logs -f flink-jobmanager flink-taskmanager

logs-service:
	docker compose logs -f cohort-service

logs-inserter:
	docker compose logs -f inserter-service

# Submit Flink job
flink-run:
	docker compose exec flink-jobmanager /opt/flink/bin/flink run -d \
		-c com.intent.cohort.CohortProcessorJob \
		/opt/flink/usrlib/cohort-processor-1.0.0.jar \
		--kafka.brokers kafka:9092

# Cancel all Flink jobs
flink-cancel:
	@for job in $$(curl -s http://localhost:8081/jobs | jq -r '.jobs[].id'); do \
		echo "Cancelling job $$job"; \
		curl -s -X PATCH "http://localhost:8081/jobs/$$job?mode=cancel"; \
	done

# Rebuild and restart Flink only
flink-restart: flink-docker
	docker compose up -d --force-recreate flink-jobmanager flink-taskmanager
	@echo "Waiting for Flink to be ready..."
	@sleep 15
	@echo "Flink restarted. Submit job with: make flink-run"

# Rebuild and restart cohort-service only
service-restart: docker
	docker compose up -d --force-recreate cohort-service

# Rebuild and restart inserter-service only
inserter-restart: docker-inserter
	docker compose up -d --force-recreate inserter-service

# Run locally (requires services running via docker compose)
run:
	POSTGRES_HOST=localhost KAFKA_BROKERS=localhost:9094 CLICKHOUSE_HOST=localhost REDIS_HOST=localhost \
	go run ./cmd/cohort-service

# Database access
psql:
	docker compose exec postgres psql -U cohort -d cohort

clickhouse:
	docker compose exec clickhouse clickhouse-client

# View Kafka topics
kafka-topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-consume-membership:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cohort.membership --from-beginning

kafka-consume-events:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events.raw --from-beginning
