kill -9 `ps -aef | grep 'redis-server' | grep -v grep | awk '{print $2}'`
