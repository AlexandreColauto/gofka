server:
	CGO_ENABLED=0 GOOS=linux go build -a -o server main.go
	docker compose up --build
clean:
	docker rm -f $$(docker ps -aq)

.PHONY: server
