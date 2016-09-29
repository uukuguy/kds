KDS_OBJS=main.go \
		 cmd/root_cmd.go \
		 cmd/server_cmd.go \
		 cmd/version_cmd.go \
		 server/mux_server.go \
		 server/object_handlers.go \
		 server/server.go \
		 server/store_server.go \
		 haystack/config.go \
		 haystack/data.go \
		 haystack/endian.go \
		 haystack/index.go \
		 haystack/io_darwin.go \
		 haystack/io_linux.go \
		 haystack/needle.go \
		 haystack/store.go \
		 haystack/superblock.go \
		 haystack/volume.go \
		 store/interfaces.go \
		 utils/bytes_utils.go \
		 utils/http_utils.go \
		 utils/logger_utils.go

all: kds

.PHONY: kds

kds: $(GOOBJS)
	go install github.com/uukuguy/kds

put:
	curl -X PUT -F file=@./data/16.txt -F vid=2 -F key=12345 -F cookie=45678 http://localhost:8709/a/b/c/d.jpg

get:
	curl -X GET "http://localhost:8709/a/b?vid=2&key=12345&cookie=45678"

test_haystack: 
	go test github.com/uukuguy/kds/haystack

clean:
	go clean github.com/uukuguy/kds
