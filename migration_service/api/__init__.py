import os

version_dict = {
    '0': 'v_1_0_1',
    '1': 'v_1_0_1',
    '20200603104139': 'latest'
}

latest = "latest"

goose_template = "goose postgres " \
                 "\"dbname={} " \
                 "user={} " \
                 "password={} " \
                 "host={} " \
                 "port={} " \
                 "sslmode=disable\" {}"

path = os.path.dirname(__file__) + "/../migration_files"
goose_migration_template = "goose -dir " + path + " postgres " \
                         "\"dbname={} " \
                         "user={} " \
                         "password={} " \
                         "host={} " \
                         "port={} " \
                         "sslmode=disable\" {}"

host = os.environ.get("MF_METADATA_DB_HOST", "localhost")
port = os.environ.get("MF_METADATA_DB_PORT", 5432)
user = os.environ.get("MF_METADATA_DB_USER", "postgres")
password = os.environ.get("MF_METADATA_DB_PSWD", "postgres")
database_name = os.environ.get("MF_METADATA_DB_NAME", "postgres")
