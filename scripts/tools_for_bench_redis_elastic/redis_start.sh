# Start and Index Redis

num_shards=$1
source=$2
echo $num_shards, $source
## start Redis
cd /users/kanwu/flashsearch/src/pysrc
python -m benchmarks.start_redis_server $num_shards &
