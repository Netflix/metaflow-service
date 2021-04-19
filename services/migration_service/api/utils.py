from subprocess import Popen, PIPE
from ..data.postgres_async_db import PostgresUtils
from . import version_dict, latest, goose_template, \
    goose_migration_template
from services.migration_service.migration_config import host, port, user, password, database_name


class ApiUtils(object):

    @staticmethod
    def list_migrations():
        migrations_list = list((version_dict.keys()))
        migrations_list.sort(key=int)
        return migrations_list[1:]

    @staticmethod
    def get_unapplied_migrations(current_version):
        try:
            migrations_list = ApiUtils.list_migrations()
            index_version = migrations_list.index(current_version)
            return migrations_list[index_version + 1:]
        except:
            return migrations_list

    @staticmethod
    async def get_goose_version():
        # if tables exist but goose doesn't find version table then
        goose_version_cmd = goose_template.format(
            database_name, user, password, host,
            port, "version"
        )

        p = Popen(goose_version_cmd, stdout=PIPE, stderr=PIPE, shell=True,
                  close_fds=True)
        p.wait()

        version = None
        std_err = p.stderr.read()
        lines_err = std_err.decode("utf-8").split("\n")
        for line in lines_err:
            if "goose: version" in line:
                s = line.split("goose: version ")
                version = s[1]
                print(line)
                break

        if version:
            return version
        else:
            raise Exception(
                "unable to get db version via goose: " + std_err.decode("utf-8"))

    @staticmethod
    async def get_latest_compatible_version():
        is_present = await PostgresUtils.is_present("flows_v3")
        if is_present:
            version = await ApiUtils.get_goose_version()
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

    @staticmethod
    async def is_migration_in_progress():
        goose_version_cmd = goose_template.format(
            database_name, user, password, host,
            port, "status"
        )

        p = Popen(goose_version_cmd, stdout=PIPE, stderr=PIPE, shell=True,
                  close_fds=True)
        p.wait()

        std_err = p.stderr.read()
        lines_err = std_err.decode("utf-8")
        if "Pending" in lines_err:
            return True

        return False
