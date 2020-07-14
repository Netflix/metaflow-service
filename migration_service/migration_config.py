import os

table = str.maketrans({"'": "\'", "`": "\`"})
host = os.environ.get("MF_METADATA_DB_HOST", "localhost").translate(table)
port = os.environ.get("MF_METADATA_DB_PORT", 5432)
user = os.environ.get("MF_METADATA_DB_USER", "postgres").translate(table)
password = os.environ.get("MF_METADATA_DB_PSWD", "postgres").translate(table)
database_name = os.environ.get("MF_METADATA_DB_NAME", "postgres").translate(table)