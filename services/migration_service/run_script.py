from subprocess import Popen
import os
if __name__ == "__main__":
    try:
        my_env = os.environ
        migration_server_process = Popen("PYTHONPATH=/ python3 -m migration_service.migration_server", shell=True,
                          close_fds=True, env=my_env)

        get_env_version = Popen(
            "python3 /migration_service/get_virtual_env.py >> /migration_service/env_output.txt",
            shell=True,
            close_fds=True)

        get_env_version.wait()

        # read in version of metadata service to load
        version_value_file = open('/migration_service/config', 'r')
        version_value = str(version_value_file.read()).strip()

        # start proper version of metadata service
        virtual_env_path = '/opt/'+version_value
        my_env['VIRTUAL_ENV'] = '/opt/'+version_value
        path = my_env['PATH']
        my_env['PATH'] = virtual_env_path+"/bin:"+path
        metadata_server_process = Popen(
            "metadata_service", shell=True,
            close_fds=True, env=my_env)

        metadata_server_process.wait()
        migration_server_process.wait()
    finally:
        # should never be reached
        exit(1)
