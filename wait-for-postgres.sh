#!/bin/bash

while !</dev/tcp/${MF_METADATA_DB_HOST}/${MF_METADATA_DB_PORT}; do
    sleep 1;
done;

$@