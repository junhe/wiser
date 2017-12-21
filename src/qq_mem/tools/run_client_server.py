import subprocess
import os
import time
import sys

if len(sys.argv) != 5:
    print "Usage: python run_client_server.py client-path arg1,arg2,.. server-path arg1,arg2,.."
    exit(1)

client = sys.argv[1]
client_args = sys.argv[2]
server = sys.argv[3]
server_args = sys.argv[4]

def main():
    cmd = [server] + server_args.split(",")
    print cmd
    server_p = subprocess.Popen(cmd)

    time.sleep(1)

    cmd = [client] + client_args.split(",")
    print cmd
    subprocess.call(cmd)

    time.sleep(1)
    server_p.wait()


if __name__ == "__main__":
    main()


