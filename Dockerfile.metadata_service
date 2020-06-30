FROM python:3.7
ADD metadata_service /root/metadata_service
ADD setup.py setup.cfg /root/
WORKDIR /root
RUN pip install .
CMD metadata_service