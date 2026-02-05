##@ Build

CONTAINER_TOOL ?= docker

IMG ?= artemis:latest
IMG_DUCKDB ?= artemis-duckdb:latest

.PHONY: build
build: ## Build lightweight binary without DuckDB support (no CGO required)
	@VERSION=$$(cat VERSION) && \
	REVISION=$$(git rev-parse HEAD) && \
	BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	BUILDUSER=$$(whoami)@$$HOSTNAME && \
	BUILDDATE=$$(date +%Y%m%d-%H:%M:%S) && \
	CGO_ENABLED=0 go build -o bin/artemis \
		-ldflags="-s -w \
		-X github.com/prometheus/common/version.Version=$$VERSION \
		-X github.com/prometheus/common/version.Revision=$$REVISION \
		-X github.com/prometheus/common/version.Branch=$$BRANCH \
		-X github.com/prometheus/common/version.BuildUser=$$BUILDUSER \
		-X github.com/prometheus/common/version.BuildDate=$$BUILDDATE" \
		cmd/artemis/main.go

.PHONY: build-duckdb
build-duckdb: ## Build binary with DuckDB SQL support (requires CGO)
	@VERSION=$$(cat VERSION) && \
	REVISION=$$(git rev-parse HEAD) && \
	BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	BUILDUSER=$$(whoami)@$$HOSTNAME && \
	BUILDDATE=$$(date +%Y%m%d-%H:%M:%S) && \
	CGO_ENABLED=1 go build -tags "duckdb duckdb_arrow" -o bin/artemis-duckdb \
		-ldflags="-s -w \
		-X github.com/prometheus/common/version.Version=$$VERSION \
		-X github.com/prometheus/common/version.Revision=$$REVISION \
		-X github.com/prometheus/common/version.Branch=$$BRANCH \
		-X github.com/prometheus/common/version.BuildUser=$$BUILDUSER \
		-X github.com/prometheus/common/version.BuildDate=$$BUILDDATE" \
		cmd/artemis/main.go

.PHONY: build-tool
build-tool: ## Build artemistool CLI for querying Artemis
	@CGO_ENABLED=0 go build -o bin/artemistool \
		-ldflags="-s -w" \
		cmd/artemistool/main.go

.PHONY: docker-build
docker-build: ## Build lightweight Docker image without DuckDB support
	@VERSION=$$(cat VERSION) && \
	REVISION=$$(git rev-parse HEAD) && \
	BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	BUILDUSER=$$(whoami)@$$HOSTNAME && \
	BUILDDATE=$$(date +%Y%m%d-%H:%M:%S) && \
	$(CONTAINER_TOOL) build --load \
		--build-arg VERSION=$$VERSION \
		--build-arg REVISION=$$REVISION \
		--build-arg BRANCH=$$BRANCH \
		--build-arg BUILDUSER=$$BUILDUSER \
		--build-arg BUILDDATE=$$BUILDDATE \
		-t ${IMG} .

.PHONY: docker-build-duckdb
docker-build-duckdb: ## Build Docker image with DuckDB SQL support (requires CGO)
	@VERSION=$$(cat VERSION) && \
	REVISION=$$(git rev-parse HEAD) && \
	BRANCH=$$(git rev-parse --abbrev-ref HEAD) && \
	BUILDUSER=$$(whoami)@$$HOSTNAME && \
	BUILDDATE=$$(date +%Y%m%d-%H:%M:%S) && \
	$(CONTAINER_TOOL) build --load -f Dockerfile.duckdb \
		--build-arg VERSION=$$VERSION \
		--build-arg REVISION=$$REVISION \
		--build-arg BRANCH=$$BRANCH \
		--build-arg BUILDUSER=$$BUILDUSER \
		--build-arg BUILDDATE=$$BUILDDATE \
		-t ${IMG_DUCKDB} .

.PHONY: deps
deps: ## Ensures fresh go.mod and go.sum.
	@go mod tidy
	@go mod verify

FILES_TO_FMT      	 ?= $(shell find . -path ./vendor -prune -o -name '*.go' -print)

.PHONY: format
format: ## Formats Go code.
format: goimports
	@echo ">> formatting code"
	go fmt ./...
	@$(GOIMPORTS) -local github.com/saswatamcode/artemis -w $(FILES_TO_FMT)

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: lint ## Runs various static analysis against our code.
lint: vet golangci-lint deps
	@echo ">> linting all of the Go files GOGC=${GOGC}"
	@$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: test
test: format vet 
	go test -race -v $$(go list ./... | grep -v /e2e) -coverprofile cover.out

.PHONY: test-e2e
test-e2e: format vet
	go test -v ./e2e -run TestArtemisE2E -timeout 10m

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

GOLANGCI_LINT_VERSION ?= v2.8.0
GOIMPORTS_VERSION ?= v0.38.0

GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)
GOIMPORTS ?= $(LOCALBIN)/goimports-$(GOIMPORTS_VERSION)

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

.PHONY: goimports
goimports: $(GOIMPORTS) ## Download goimports locally if necessary.
$(GOIMPORTS): $(LOCALBIN)
	$(call go-install-tool,$(GOIMPORTS),golang.org/x/tools/cmd/goimports,$(GOIMPORTS_VERSION))


# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary (ideally with version)
# $2 - package url which can be installed
# $3 - specific version of package
# $4 - additional flags for go install
define go-install-tool
@[ -f $(1) ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
GOBIN=$(LOCALBIN) go install $${package} $${4} ;\
mv "$$(echo "$(1)" | sed "s/-$(3)$$//")" $(1) ;\
}
endef
