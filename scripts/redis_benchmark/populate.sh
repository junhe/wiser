KEYSPACE=$1
VALUESIZE=$2
~/redis/src/redis-cli flushall
echo KEYSPACE: $KEYSPACE Value_Size: $VALUESIZE
./memtier_benchmark --hide-histogram --key-maximum=$KEYSPACE -n allkeys -d $VALUESIZE --key-pattern=P:P --ratio=1:0
