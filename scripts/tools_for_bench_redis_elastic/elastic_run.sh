# kill
./elastic_kill.sh

# clean cache
free -h && sync && echo 3 > /proc/sys/vm/drop_caches && free -h

# start 
