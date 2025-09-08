.PHONY: run-all visualizer server client clean

# Run all components in sequence with delays
run-all:
	@echo "Starting Kafka implementation..."
	docker network ls | grep gofka-networks > /dev/null || docker network create gofka-networks
	@echo "1. Starting visualizer..."
	cd visualizer_server && make visualizer &
	@sleep 1
	@echo "2. Starting server..."
	cd server && make server &
	@sleep 1
	@echo "3. Starting client..."
	cd client && make client

# Individual targets
visualizer:
	cd visualizer_server && make visualizer

server:
	cd server && make server

client:
	cd client && make client

# Stop all processes (if needed)
stop:
	@echo "Stopping all Docker Compose services..."
	@cd visualizer_server && docker compose down || true
	@cd server && docker compose down || true
	@cd client && docker compose down || true
	@echo "Removing network if it exists..."
	@docker network rm gofka-networks 2>/dev/null || true

clean:
	cd server && make clean || true
