FROM golang:latest

RUN go get -u github.com/pressly/goose/cmd/goose

RUN apt-get update && apt-get -y install python3.7 && apt-get -y install python3-pip && apt-get -y install libpq-dev

RUN pip3 install virtualenv && pip3 install requests

RUN virtualenv /opt/v_1_0_1 -p python3
RUN virtualenv /opt/latest -p python3

RUN /opt/v_1_0_1/bin/pip install https://github.com/Netflix/metaflow-service/archive/1.0.1.zip

ADD metadata_service /root/metadata_service
ADD setup.py setup.cfg /root/
WORKDIR /root
RUN /opt/latest/bin/pip install .

# Migration Service
ADD migration_service /migration_service
RUN pip3 install -r /migration_service/requirements.txt

RUN chmod 777 /migration_service/run_script.py
CMD python3 /migration_service/run_script.py
