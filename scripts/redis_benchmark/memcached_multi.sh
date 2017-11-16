num_server=$1
port_last=$(($1+7777))
echo overall

for ((port=7778; port <= $port_last; ++port))
do
./get_benchmark.sh 10000 $port &
echo ./get_benchmark.sh 10000 $port
done


exit 0
