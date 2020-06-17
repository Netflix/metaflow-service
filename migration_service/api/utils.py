from subprocess import Popen, PIPE
from ..data.postgres_async_db import PostgresUtils
from . import version_dict, latest, goose_template, \
    host, port, user, password, database_name, goose_migration_template


class ApiUtils(object):
    @staticmethod
    async def get_goose_version():
        is_present = await PostgresUtils.is_present("flows_v3")
        version = "unknown"
        if is_present:
            # if tables exist but goose doesn't find version table then
            goose_version_cmd = goose_template.format(
                database_name, user, password, host,
                port, "version"
            )

            p = Popen(goose_version_cmd, stdout=PIPE, stderr=PIPE, shell=True,
                      close_fds=True)
            p.wait()

            std_err = p.stderr.read()
            lines_err = std_err.decode("utf-8").split("\n")
            for line in lines_err:
                if "goose: version" in line:
                    s = line.split("goose: version ")
                    version = s[1]
                    print(line)
                    break
            return version_dict[version]
        else:
            goose_version_cmd = goose_migration_template.format(
                database_name, user, password, host, port,
                "up"
            )
            p = Popen(goose_version_cmd, shell=True,
                      close_fds=True)
            p.wait()
            return latest
