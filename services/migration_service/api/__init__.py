import os

version_dict = {
    '0': 'v_1_0_1',
    '1': 'v_1_0_1',
    '20200603104139': '20200603104139',
    '20201002000616': '20201002000616',
    '20210202145952': 'latest'
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
