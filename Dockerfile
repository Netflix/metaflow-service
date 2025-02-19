FROM --platform=$BUILDPLATFORM golang:1.20.2-buster AS goose
ARG TARGETOS
ARG TARGETARCH

ENV GOOS=$TARGETOS
ENV GOARCH=$TARGETARCH
ENV CGO_ENABLED=0

WORKDIR /tmp/goose-src
RUN go mod init goose-tmp
RUN go get github.com/pressly/goose/v3@v3.9.0
RUN go mod tidy
RUN go build -o /go/bin/goose github.com/pressly/goose/v3/cmd/goose


FROM python:3.11.6-slim-bookworm
COPY --from=goose /go/bin/goose /usr/local/bin/

ARG BUILD_TIMESTAMP
ARG BUILD_COMMIT_HASH
ENV BUILD_TIMESTAMP=$BUILD_TIMESTAMP
ENV BUILD_COMMIT_HASH=$BUILD_COMMIT_HASH

ARG UI_ENABLED="1"
ARG UI_VERSION="v1.3.13"
ENV UI_ENABLED=$UI_ENABLED
ENV UI_VERSION=$UI_VERSION

ENV FEATURE_RUN_GROUPS=0
ENV FEATURE_DEBUG_VIEW=1

RUN apt-get update -y \
    && apt-get -y install libpq-dev unzip gcc curl

RUN pip3 install virtualenv requests

# TODO: possibly unused virtualenv. See if it can be removed
RUN virtualenv /opt/v_1_0_1 -p python3
# All of the official deployment templates reference this virtualenv for launching services.
RUN virtualenv /opt/latest -p python3

RUN /opt/v_1_0_1/bin/pip install https://github.com/Netflix/metaflow-service/archive/1.0.1.zip

ADD services/__init__.py /root/services/
ADD services/data/service_configs.py /root/services/
ADD services/data /root/services/data
ADD services/metadata_service /root/services/metadata_service
ADD services/ui_backend_service /root/services/ui_backend_service
ADD services/utils /root/services/utils
ADD setup.py setup.cfg run_goose.py /root/
WORKDIR /root
RUN /opt/latest/bin/pip install .

# Install Netflix/metaflow-ui release artifact
RUN /root/services/ui_backend_service/download_ui.sh

# Migration Service
ADD services/migration_service /root/services/migration_service
RUN pip3 install -r /root/services/migration_service/requirements.txt

RUN chmod 777 /root/services/migration_service/run_script.py
CMD python3  services/migration_service/run_script.py
