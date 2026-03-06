# =============================================================================
# Unified Dockerfile for metaflow-service
#
# Replaces: Dockerfile.metadata_service, Dockerfile.migration_service,
#           Dockerfile.ui_service, Dockerfile.service.test
#
# Stages:
#   goose   - builds the goose binary from Go source
#   base    - shared foundation for all services
#   runtime - used by metadata, migration, and ui_backend in dev & production
#   test    - used by the test runner (tox)
# =============================================================================

# -----------------------------------------------------------------------------
# GOOSE stage — builds the goose DB migration binary from source
# -----------------------------------------------------------------------------
FROM golang:1.20.2 AS goose

RUN go install github.com/pressly/goose/v3/cmd/goose@v3.9.0

# -----------------------------------------------------------------------------
# BASE stage — shared by all services
# -----------------------------------------------------------------------------
FROM python:3.11.7-bookworm AS base

COPY --from=goose /go/bin/goose /usr/local/bin/

# libpq-dev + gcc: required by psycopg2
# unzip + curl: required by download_ui.sh
# dos2unix: fixes Windows CRLF line endings in shell scripts
RUN apt-get update -y \
    && apt-get -y install libpq-dev gcc unzip curl dos2unix \
    && rm -rf /var/lib/apt/lists/*

# Build args for ui_backend_service
ARG UI_ENABLED="1"
ARG UI_VERSION="v1.3.13"
ARG BUILD_TIMESTAMP
ARG BUILD_COMMIT_HASH
ARG CUSTOM_QUICKLINKS

ENV UI_ENABLED=$UI_ENABLED
ENV UI_VERSION=$UI_VERSION
ENV BUILD_TIMESTAMP=$BUILD_TIMESTAMP
ENV BUILD_COMMIT_HASH=$BUILD_COMMIT_HASH
ENV CUSTOM_QUICKLINKS=$CUSTOM_QUICKLINKS

ADD services/__init__.py /root/services/
ADD services/data /root/services/data
ADD services/utils /root/services/utils
ADD services/metadata_service /root/services/metadata_service
ADD services/migration_service /root/services/migration_service
ADD services/ui_backend_service /root/services/ui_backend_service
ADD setup.py setup.cfg run_goose.py MANIFEST.in /root/

WORKDIR /root

RUN pip install --editable .
RUN chmod 777 /root/services/migration_service/run_script.py

# -----------------------------------------------------------------------------
# RUNTIME stage — used by metadata, migration, and ui_backend
# dos2unix fixes Windows CRLF line endings before executing the script
# When UI_ENABLED=0, the script prints "UI not enabled" and exits cleanly
# -----------------------------------------------------------------------------
FROM base AS runtime

RUN dos2unix /root/services/ui_backend_service/download_ui.sh \
    && bash /root/services/ui_backend_service/download_ui.sh

CMD ["metadata_service"]

# -----------------------------------------------------------------------------
# TEST stage — used by the test runner only
# Inherits from base so download_ui.sh is never called
# dos2unix fixes wait-for-postgres.sh line endings too
# -----------------------------------------------------------------------------
FROM base AS test

RUN pip install tox

WORKDIR /app
COPY . /app

RUN dos2unix /app/wait-for-postgres.sh

CMD /app/wait-for-postgres.sh tox
