KEYSPACE=$1
echo KEYSPACE: $KEYSPACE
./memtier_benchmark --hide-histogram -n 10000 --ratio=0:1 --key-maximum=$KEYSPACE
