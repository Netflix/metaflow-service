FROM python:3.7
ADD metadata_service /metadata_service
RUN pip3 install -r /metadata_service/requirements.txt
RUN cd /
CMD python3 -m metadata_service.server