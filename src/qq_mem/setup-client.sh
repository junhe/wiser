# setup vacuum client env
./setup.sh

# install go
cd ../../scripts
sudo ./install-go.sh
cd -

source ~/.bashrc

# setup redisearch go bench
cd ../../scripts && ./setup-redisearchbench.sh && cd -


