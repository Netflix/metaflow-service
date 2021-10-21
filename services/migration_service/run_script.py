from subprocess import Popen
import os


def setup_env(version_value: str):
    _env = os.environ
    virtual_env_path = '/opt/' + version_value
    _env['VIRTUAL_ENV'] = virtual_env_path
    path = _env['PATH']
    _env['PATH'] = virtual_env_path + "/bin:" + path
    return _env


if __name__ == "__main__":
    try:
        migration_server_process = Popen(
            "PYTHONPATH=/ python3 -m services.migration_service.migration_server",
            shell=True,
            close_fds=True,
            env=setup_env('latest')
        )

        get_env_version = Popen(
            "python3 -m services.migration_service.get_virtual_env",
            shell=True,
            close_fds=True
        )

        get_env_version.wait()

        # read in version of metadata service to load
        version_value_file = open('/root/services/migration_service/config', 'r')
        version_value = str(version_value_file.read()).strip()

        # start proper version of metadata service
        metadata_server_process = Popen(
            "metadata_service",
            shell=True,
            close_fds=True,
            env=setup_env(version_value)
        )

        metadata_server_process.wait()
        migration_server_process.wait()
    except Exception as e:
        print(e)
    finally:
        # should never be reached
        exit(1)
