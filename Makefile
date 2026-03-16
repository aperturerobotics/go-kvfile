# https://github.com/aperturerobotics/template
SHELL:=bash
APTRE := go run -mod=mod github.com/aperturerobotics/common/cmd/aptre

export GO111MODULE=on
undefine GOARCH
undefine GOOS

all:

vendor:
	go mod vendor

.PHONY: genproto
genproto:
	$(APTRE) generate

.PHONY: gen
gen: genproto

.PHONY: lint
lint:
	$(APTRE) lint

.PHONY: fix
fix:
	$(APTRE) fix

.PHONY: format
format:
	$(APTRE) format

.PHONY: test
test:
	$(APTRE) test

.PHONY: outdated
outdated:
	$(APTRE) outdated
