num_server=$1
port_last=$(($1+7777))
num_threads=$2
for ((port=7778; port <= $port_last; ++port))
do
/users/kanwu/memcached/memcached -p $port -M -v -m 65536 -t $num_threads -A &
echo /users/kanwu/memcached/memcached -p $port -M -v -m 65536 -t $num_threads -A
sleep 3
/users/kanwu/memtier_benchmark/populate.sh 10000 4096 $port
done

exit 0
