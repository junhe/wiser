import subprocess
import os
import time
import sys

if len(sys.argv) != 3:
    print "Usage: python run_client_server.py client-path server-path"
    exit(1)

client = sys.argv[1]
server = sys.argv[2]

def main():
    server_p = subprocess.Popen([server])

    time.sleep(1)
    subprocess.call([client])

    time.sleep(1)
    server_p.wait()


if __name__ == "__main__":
    main()


