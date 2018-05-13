# setup vacuum client env
./setup.sh

# install go
cd ../../scripts
./install-go.sh
cd -

# setup redisearch go bench
cd ../../scripts
./setup-redisearchbench.sh
cd -


