.PHONY: storage frontend client tracing-server config-gen clean

all: storage frontend client tracing-server

storage:
	go build -o storage cmd/storage/main.go

frontend:
	go build -o frontend cmd/frontend/main.go

client:
	go build -o client cmd/client/main.go

tracing-server:
	go build -o tracing-server cmd/tracing-server/main.go

clean:
	rm storage frontend client tracing-server *".log" *"-Log.txt" 2> /dev/null || true
