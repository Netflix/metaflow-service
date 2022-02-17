import os
import sys
import requests
import socket
import time
from services.data.service_configs import max_startup_retries, \
    startup_retry_wait_time_seconds

port = int(os.environ.get("MF_MIGRATION_PORT", 8082))

try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    retry_count = max_startup_retries
    while retry_count > 0:
        print(retry_count)
        try:
            print("connecting")
            s.connect(('localhost', port))
            print("Port reachable", port)
            break
        except socket.error as e:
            print("booting...")
            print(e)
            time.sleep(startup_retry_wait_time_seconds)
        except Exception:
            print("something broke")
        finally:
            retry_count = retry_count - 1
    # continue
    s.close()
    if retry_count == 0:
        print("ran out of retries to get migration version, exiting")
        sys.exit(1)
except Exception as e:
    print(e)
    sys.exit(1)

r = requests.get('http://localhost:{0}/version'.format(port))
r.raise_for_status()

conf_file = open('/root/services/migration_service/config', 'w')
print(r.text, file=conf_file)
conf_file.close()
