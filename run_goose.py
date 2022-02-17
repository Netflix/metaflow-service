from subprocess import Popen
import os
from urllib.parse import quote

if __name__ == '__main__':
    db_connection_string = f"postgresql://{quote(os.environ['MF_METADATA_DB_USER'])}:{quote(os.environ['MF_METADATA_DB_PSWD'])}@{os.environ['MF_METADATA_DB_HOST']}:{os.environ['MF_METADATA_DB_PORT']}/{os.environ['MF_METADATA_DB_NAME']}?sslmode=disable"

    p = Popen(["/go/bin/goose", '-dir', '/root/services/migration_service/migration_files/', "postgres", db_connection_string, "up"])
    if p.wait() != 0:
        raise Exception("Failed to run initial migration")