# =============================================================================
# Unified Dockerfile for metaflow-service
#
# Replaces: Dockerfile.metadata_service, Dockerfile.migration_service,
#           Dockerfile.ui_service, Dockerfile.service.test
#
# Stages:
#   goose   - builds the goose binary (multiarch: amd64 + arm64)
#   base    - shared foundation, regular pip install (production-safe)
#   dev     - extends base with editable install for hot-reload in development
#   runtime - production image, extends base, downloads UI
#   test    - extends base, adds tox for running tests locally
# =============================================================================

# -----------------------------------------------------------------------------
# GOOSE stage — multiarch Go builder
# Supports both amd64 (Intel/AMD) and arm64 (Apple M1/M2, Raspberry Pi)
# Pattern preserved from original production Dockerfile
# -----------------------------------------------------------------------------
FROM golang:1.20.2-buster AS amd64-golang
FROM arm64v8/golang:1.20.2-buster AS arm64-golang

FROM ${TARGETARCH}-golang AS goose
RUN go install github.com/pressly/goose/v3/cmd/goose@v3.9.0

# -----------------------------------------------------------------------------
# BASE stage — shared by all stages
# Uses regular (non-editable) pip install, safe for both dev and production
# -----------------------------------------------------------------------------
FROM python:3.11.6-slim-bookworm AS base

COPY --from=goose /go/bin/goose /usr/local/bin/

# Build args — preserved from original production Dockerfile
ARG BUILD_TIMESTAMP
ARG BUILD_COMMIT_HASH
ENV BUILD_TIMESTAMP=$BUILD_TIMESTAMP
ENV BUILD_COMMIT_HASH=$BUILD_COMMIT_HASH

ARG UI_ENABLED="1"
ARG UI_VERSION="v1.3.13"
ENV UI_ENABLED=$UI_ENABLED
ENV UI_VERSION=$UI_VERSION

ARG CUSTOM_QUICKLINKS
ENV CUSTOM_QUICKLINKS=$CUSTOM_QUICKLINKS

ENV FEATURE_RUN_GROUPS=0
ENV FEATURE_DEBUG_VIEW=1

# libpq-dev + gcc: required by psycopg2 (PostgreSQL adapter)
# unzip + curl: required by download_ui.sh
RUN apt-get update -y \
    && apt-get -y install libpq-dev gcc unzip curl \
    && rm -rf /var/lib/apt/lists/*

ADD services/__init__.py /root/services/
ADD services/data /root/services/data
ADD services/utils /root/services/utils
ADD services/metadata_service /root/services/metadata_service
ADD services/migration_service /root/services/migration_service
ADD services/ui_backend_service /root/services/ui_backend_service
ADD setup.py setup.cfg run_goose.py MANIFEST.in /root/

WORKDIR /root

# Regular install — production-safe, no symlinks, no dependency on source tree
RUN pip install .

RUN chmod 777 /root/services/migration_service/run_script.py

# -----------------------------------------------------------------------------
# DEV stage — extends base with editable install
# Used by docker-compose.development.yml only
# Editable install means volume-mounted code changes reflect immediately
# without needing to rebuild the image
# -----------------------------------------------------------------------------
FROM base AS dev

RUN pip install --editable .

CMD ["metadata_service"]

# -----------------------------------------------------------------------------
# RUNTIME stage — production image
# Extends base (regular install), downloads the React UI build
# Used by docker-compose.yml (production)
# -----------------------------------------------------------------------------
FROM base AS runtime

# Download the React UI build
# When UI_ENABLED=0 the script skips the download cleanly
RUN bash /root/services/ui_backend_service/download_ui.sh

CMD ["metadata_service"]

# -----------------------------------------------------------------------------
# TEST stage — local test runner
# Extends base (not dev or runtime), adds tox
# Matches original Dockerfile.service.test exactly
# -----------------------------------------------------------------------------
FROM base AS test

RUN pip install tox

WORKDIR /app
COPY . /app

CMD /app/wait-for-postgres.sh tox
