KEYSPACE=$1
VALUESIZE=$2
PORT=$3
echo KEYSPACE: $KEYSPACE Value_Size: $VALUESIZE
# random size
#./memtier_benchmark -p 8888 --hide-histogram -p $PORT --data-size-range=1024-8192 --data-size-pattern=R --key-maximum=$KEYSPACE -n allkeys --key-pattern=P:P --ratio=1:0

# eual size value
#/users/kanwu/memtier_benchmark/memtier_benchmark -p $PORT --hide-histogram --key-maximum=$KEYSPACE -n allkeys -d $VALUESIZE --key-pattern=P:P --ratio=1:0

# memcached
/users/kanwu/memtier_benchmark/memtier_benchmark --protocol=memcache_text -p $PORT --hide-histogram --key-maximum=$KEYSPACE -n allkeys -d $VALUESIZE --key-pattern=P:P --ratio=1:0 --key-prefix=""

# random size
#./memtier_benchmark -p 8888 --hide-histogram -p $PORT --protocol=memcache_binary --data-size-range=1024-8192 --data-size-pattern=R --key-maximum=$KEYSPACE -n allkeys --key-pattern=P:P --ratio=1:0

