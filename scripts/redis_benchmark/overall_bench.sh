KEYSPACE=$1
VALUESIZE=$2
./populate.sh $1 $2
./get_benchmark.sh $1
