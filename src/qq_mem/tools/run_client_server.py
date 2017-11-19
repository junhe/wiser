import subprocess
import os
import time

build_dir = "../build/"
client = os.path.join(build_dir, "qq_client")
server = os.path.join(build_dir, "qq_server")


def main():
    server_p = subprocess.Popen([server])

    time.sleep(1)
    subprocess.call([client])

    time.sleep(1)
    server_p.wait()


if __name__ == "__main__":
    main()


