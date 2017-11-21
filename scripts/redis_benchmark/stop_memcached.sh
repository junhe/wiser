num_server=$1
port_last=$(($1+7777))
for ((port=7778; port <= $port_last; ++port))
do
echo 'shutdown' | nc localhost $port
done
exit 0
