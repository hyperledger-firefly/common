VGO=go
GOFILES := $(shell find pkg -name '*.go' -print)
GOBIN := $(shell $(VGO) env GOPATH)/bin
LINT := $(GOBIN)/golangci-lint
MOCKERY := $(GOBIN)/mockery

# Expect that FireFly compiles with CGO disabled
CGO_ENABLED=0
GOGC=30

.DELETE_ON_ERROR:

all: build test go-mod-tidy
test: deps lint
		$(VGO) test ./pkg/... -cover -coverprofile=coverage.txt -covermode=atomic -timeout=30s
coverage.html:
		$(VGO) tool cover -html=coverage.txt
coverage: test coverage.html
lint: ${LINT}
		GOGC=20 $(LINT) run -v --timeout 5m --fast --allow-parallel-runners
${MOCKERY}:
		$(VGO) install github.com/vektra/mockery/v2@latest
${LINT}:
		$(VGO) install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8
dbmigrate:
		$(eval DBMIGRATE_PATH := $(shell $(VGO) list -f '{{.Dir}}' github.com/golang-migrate/migrate/v4/database))


define makemock
mocks: mocks-$(strip $(1))-$(strip $(2))
mocks-$(strip $(1))-$(strip $(2)): ${MOCKERY} dbmigrate
	${MOCKERY} --case underscore --dir $(1) --name $(2) --outpkg $(3) --output mocks/$(strip $(3))
endef

$(eval $(call makemock, $$(DBMIGRATE_PATH),        Driver,             dbmigratemocks))
$(eval $(call makemock, pkg/httpserver,            GoHTTPServer,       httpservermocks))
$(eval $(call makemock, pkg/auth,                  Plugin,             authmocks))
$(eval $(call makemock, pkg/wsserver,              Protocol,           wsservermocks))
$(eval $(call makemock, pkg/wsserver,              WebSocketServer,    wsservermocks))
$(eval $(call makemock, pkg/dbsql,                 CRUD,               crudmocks))

firefly-common: ${GOFILES}
		$(VGO) build ./pkg/*
go-mod-tidy: .ALWAYS
		$(VGO) mod tidy
build: firefly-common
.ALWAYS: ;
clean:
		$(VGO) clean
deps:
		$(VGO) get ./pkg/*
