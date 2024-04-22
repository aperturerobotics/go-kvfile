# https://github.com/aperturerobotics/template

SHELL:=bash
PROTOWRAP=hack/bin/protowrap
PROTOC_GEN_GO=hack/bin/protoc-gen-go-lite
GOIMPORTS=hack/bin/goimports
GOFUMPT=hack/bin/gofumpt
GOLANGCI_LINT=hack/bin/golangci-lint
GO_MOD_OUTDATED=hack/bin/go-mod-outdated
GOLIST=go list -f "{{ .Dir }}" -m

export GO111MODULE=on
undefine GOARCH
undefine GOOS

all:

vendor:
	go mod vendor

$(PROTOC_GEN_GO):
	cd ./hack; \
	go build -v \
		-o ./bin/protoc-gen-go-lite \
		github.com/aperturerobotics/protobuf-go-lite/cmd/protoc-gen-go-lite

$(GOIMPORTS):
	cd ./hack; \
	go build -v \
		-o ./bin/goimports \
		golang.org/x/tools/cmd/goimports

$(GOFUMPT):
	cd ./hack; \
	go build -v \
		-o ./bin/gofumpt \
		mvdan.cc/gofumpt

$(PROTOWRAP):
	cd ./hack; \
	go build -v \
		-o ./bin/protowrap \
		github.com/aperturerobotics/goprotowrap/cmd/protowrap

$(GOLANGCI_LINT):
	cd ./hack; \
	go build -v \
		-o ./bin/golangci-lint \
		github.com/golangci/golangci-lint/cmd/golangci-lint

$(GO_MOD_OUTDATED):
	cd ./hack; \
	go build -v \
		-o ./bin/go-mod-outdated \
		github.com/psampaz/go-mod-outdated

node_modules:
	yarn install

.PHONY: genproto
genproto: vendor node_modules $(GOIMPORTS) $(PROTOWRAP) $(PROTOC_GEN_GO)
	shopt -s globstar; \
	set -eo pipefail; \
	export PROJECT=$$(go list -m); \
	export PATH=$$(pwd)/hack/bin:$${PATH}; \
	export OUT=$$(pwd)/vendor; \
	mkdir -p $${OUT}/$$(dirname $${PROJECT}); \
	rm $$(pwd)/vendor/$${PROJECT} || true; \
	ln -s $$(pwd) $$(pwd)/vendor/$${PROJECT} ; \
	protogen() { \
		$(PROTOWRAP) \
			-I $${OUT} \
			--go-lite_out=$${OUT} \
			--go-lite_opt=features=marshal+unmarshal+size+equal+json+clone \
			--proto_path $${OUT} \
			--print_structure \
			--only_specified_files \
			$$(\
				git \
					ls-files "$$1" |\
					xargs printf -- \
					"$$(pwd)/vendor/$${PROJECT}/%s "); \
	}; \
	protogen "./*.proto"; \
	rm $$(pwd)/vendor/$${PROJECT} || true
	$(GOIMPORTS) -w ./

.PHONY: gen
gen: genproto

.PHONY: outdated
outdated: $(GO_MOD_OUTDATED)
	go list -mod=mod -u -m -json all | $(GO_MOD_OUTDATED) -update -direct

.PHONY: list
list: $(GO_MOD_OUTDATED)
	go list -mod=mod -u -m -json all | $(GO_MOD_OUTDATED)

.PHONY: lint
lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run

.PHONY: fix
fix: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --fix

.PHONY: test
test:
	go test -v ./...

.PHONY: format
format: $(GOFUMPT) $(GOIMPORTS)
	$(GOIMPORTS) -w ./
	$(GOFUMPT) -w ./
