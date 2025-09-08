.PHONY:  visualizer 

visualizer:
	CGO_ENABLED=0 GOOS=linux go build -a -o visualizer_server main.go
	docker compose up --build
