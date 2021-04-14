import os

max_connection_retires = int(os.environ.get("MF_SERVICE_CONNECTION_RETRIES", 3))
connection_retry_wait_time_seconds = int(os.environ.get("MF_SERVICE_CONNECTION_RETRY_WAITTIME_SECONDS", 1))
max_startup_retries = int(os.environ.get("MF_SERVICE_STARTUP_RETRIES", 5))
startup_retry_wait_time_seconds = int(os.environ.get("MF_SERVICE_STARTUP_WAITTIME_SECONDS", 1))
