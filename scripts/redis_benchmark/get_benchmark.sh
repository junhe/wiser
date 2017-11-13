KEYSPACE=$1
PORT=$2
echo KEYSPACE: $KEYSPACE
# uniform random
#/users/kanwu/memtier_benchmark/memtier_benchmark -p $PORT --run-count=5 -c 50 --threads=4 --hide-histogram -n 10000 --ratio=0:1 --key-pattern=R:R --key-maximum=$KEYSPACE
# sequential
#./memtier_benchmark --run-count=5 -c 50 --threads=4 --hide-histogram -n 10000 --ratio=0:1 --key-pattern=R:S --key-maximum=$KEYSPACE
# gauss
#./memtier_benchmark --run-count=5 -c 50 --threads=4 --hide-histogram -n 10000 --ratio=0:1 --key-pattern=R:G --key-maximum=$KEYSPACE

#memcached
# uniform random
/users/kanwu/memtier_benchmark/memtier_benchmark -P memcache_text -p $PORT --run-count=5 -c 50 --threads=8 --hide-histogram -n 10000 --ratio=0:1 --key-pattern=R:R --key-maximum=$KEYSPACE --key-prefix=""

#sequential
#/users/kanwu/memtier_benchmark/memtier_benchmark --protocol=memcache_text -p $PORT --run-count=5 -c 50 --threads=4 --hide-histogram -n 100000 --ratio=0:1 --key-pattern=R:S --key-maximum=$KEYSPACE

# gauss
#/users/kanwu/memtier_benchmark/memtier_benchmark --run-count=5 --protocol=memcache_text -p $PORT -c 1 --threads=1 --hide-histogram -n 100000 --ratio=0:1 --key-pattern=R:G --key-maximum=$KEYSPACE
