import os

version_dict = {
    '0': 'v_1_0_1',
    '1': 'v_1_0_1',
    '20200603104139': 'latest',
    '20201002000616': 'latest'
}

latest = "latest"

goose_template = "goose postgres " \
                 "\"{}\" {}"

path = os.path.dirname(__file__) + "/../migration_files"
goose_migration_template =  "goose -dir " + path + " postgres " \
    "\"{}\" {}"
