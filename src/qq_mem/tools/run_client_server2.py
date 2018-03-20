import subprocess
import os
import time
import sys

if len(sys.argv) != 6:
    print "Usage: python run_client_server.py client-path '-flag1 3 -flag2 3' server-path '-flagx 3' 30"
    exit(1)

client = sys.argv[1]
client_args = sys.argv[2]
server = sys.argv[3]
server_args = sys.argv[4]
wait_time = int(sys.argv[5])

def main():
    cmd = [server] + server_args.split()
    print cmd
    server_p = subprocess.Popen(cmd)

    time.sleep(wait_time)

    cmd = [client] + client_args.split()
    print cmd
    subprocess.call(cmd)

    time.sleep(2)
    # server_p.wait()
    print "Killing server....."
    server_p.terminate()


if __name__ == "__main__":
    main()


