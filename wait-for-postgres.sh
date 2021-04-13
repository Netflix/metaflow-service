#!/bin/bash
RETRIES=1;
MAX_RETRIES=${POSTGRES_WAIT_MAX_RETRIES:=5};
SLEEP_SECONDS=${POSTGRES_WAIT_SLEEP_SECONDS:=1};

# Retry loop for postgres server.
while !</dev/tcp/${MF_METADATA_DB_HOST}/${MF_METADATA_DB_PORT}; do
    if (($RETRIES <= $MAX_RETRIES)); then
        echo "retry $RETRIES out of $MAX_RETRIES"
        RETRIES=$((RETRIES+1))
        sleep $SLEEP_SECONDS;
    else
        echo "Waiting for postgres server timed out."; exit 1;
    fi
done;

$@