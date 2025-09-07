server:
	CGO_ENABLED=0 GOOS=linux go build -a -o server main.go
	sudo docker compose up --build
clean:
	sudo docker rm -f $$(sudo docker ps -aq)

.PHONY: server
