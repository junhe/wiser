num_server=$1
port_last=$(($1+7777))
for ((port=7778; port <= $port_last; ++port))
do
/users/kanwu/redis/src/redis-server --port $port &
echo /users/kanwu/redis/src/redis-server --port $port
sleep 5
/users/kanwu/redis/src/redis-cli -p $port flushall
./populate.sh 10000 4096 $port
done

exit 0
