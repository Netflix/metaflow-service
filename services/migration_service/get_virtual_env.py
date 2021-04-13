import os
import requests
import socket
import time

port = int(os.environ.get("MF_MIGRATION_PORT", 8082))

try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    retry_count = 5
    while retry_count > 0:
        try:
            print("connecting")
            s.connect(('localhost', port))
            print("Port reachable", port)
            break
        except socket.error as e:
            print("booting...")
            print(e)
            time.sleep(1)
        except Exception:
            print("something broke")
        finally:
            retry_count = retry_count - 1
    # continue
    s.close()
except Exception:
    pass

r = requests.get('http://localhost:{0}/version'.format(port))
conf_file = open('/migration_service/config', 'w')
print(r.text, file=conf_file)
conf_file.close()
