FROM golang:1.16.3

ARG BUILD_TIMESTAMP
ARG BUILD_COMMIT_HASH
ENV BUILD_TIMESTAMP=$BUILD_TIMESTAMP
ENV BUILD_COMMIT_HASH=$BUILD_COMMIT_HASH

ARG UI_ENABLED="1"
ARG UI_VERSION="v1.0.0"
ENV UI_ENABLED=$UI_ENABLED
ENV UI_VERSION=$UI_VERSION

ENV FEATURE_RUN_GROUPS=0
ENV FEATURE_DEBUG_VIEW=1

RUN go get -u github.com/pressly/goose/cmd/goose

RUN apt-get update && apt-get -y install python3.7 && apt-get -y install python3-pip && apt-get -y install libpq-dev unzip

RUN pip3 install virtualenv && pip3 install requests

RUN virtualenv /opt/v_1_0_1 -p python3
RUN virtualenv /opt/latest -p python3

RUN /opt/v_1_0_1/bin/pip install https://github.com/Netflix/metaflow-service/archive/1.0.1.zip

ADD services/__init__.py /root/services/
ADD services/data/service_configs.py /root/services/
ADD services/data /root/services/data
ADD services/metadata_service /root/services/metadata_service
ADD services/ui_backend_service /root/services/ui_backend_service
ADD services/utils /root/services/utils
ADD setup.py setup.cfg /root/
WORKDIR /root
RUN /opt/latest/bin/pip install .

# Install Netflix/metaflow-ui release artifact
RUN /root/services/ui_backend_service/download_ui.sh

# Migration Service
ADD services/migration_service /root/services/migration_service
RUN pip3 install -r /root/services/migration_service/requirements.txt

RUN chmod 777 /root/services/migration_service/run_script.py
CMD python3  services/migration_service/run_script.py
