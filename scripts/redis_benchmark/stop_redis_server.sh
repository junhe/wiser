num_server=$1
port_last=$(($1+7777))
for ((port=7778; port <= $port_last; ++port))
do
/users/kanwu/redis/src/redis-cli -p $port shutdown
echo /users/kanwu/redis/src/redis-cli -p $port shutdown
done
exit 0
