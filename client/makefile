.PHONY: client 

client:
	CGO_ENABLED=0 GOOS=linux go build -a -o client main.go
	docker compose up --build
