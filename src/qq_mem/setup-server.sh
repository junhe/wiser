REMOTE=jhe@something.com
ES_INDEX_PATH=/mnt/ssd/elasticsearch


# install java
cd .././scripts/ && ./install-java.sh
cd -

# copy elasticsearch from remote machine
rsync -avh --progress $REMOTE:./elasticsearch-5.6.3 ~/

# copy elasticsearch index
rsync -avh --progress $REMOTE:$ES_INDEX_PATH /mnt/ssd/

# copy vacuum index
rsync -avh --progress $REMOTE:/mnt/ssd/elasticsearch /mnt/ssd/
rsync -avh --progress $REMOTE:/mnt/ssd/vacuum_0511_chunked_doc_store /mnt/ssd/
rsync -avh --progress $REMOTE:/mnt/ssd/vacuum_0501_prefetch_zone /mnt/ssd/
rsync -avh --progress $REMOTE:/mnt/ssd/vacuum_delta_skiplist-04-30 /mnt/ssd/

# setup vacuum running env
./setup.sh


