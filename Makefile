KDS_OBJS=main.go \
		 cmd/root_cmd.go \
		 cmd/server_cmd.go \
		 cmd/version_cmd.go \
		 server/mux_server.go \
		 server/object_handlers.go \
		 server/server.go \
		 server/stack_server.go

all: kds

.PHONY: kds

kds: $(GOOBJS)
	go install github.com/uukuguy/kds
