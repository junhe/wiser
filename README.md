# flashsearch

# How to setup an engine search

1. install java

```
cd scripts
./install-java.sh
```

2. copy elasticsearch files

```
rsync -avh ~/elasticsearch-5.6.3 REMOTE:./
```

3. copy elasticsearch index
4. copy vacuum index
5. setup vacuum

```
cd ~/flashsearch/src/qq_mem
./setup.sh
```

# How to setup an engine client

1. install go
2. setup redisearch bench
3. setup vacuum (you need the environment such as GLOG)

```
cd ~/flashsearch/src/qq_mem
./setup.sh
```

# How to Install Redis

```
cd ./scripts/
./setup-redis.sh
source ~/.bashrc
```

Now you can just use command `redis` in command line.

# How to Install RediSearch

```
cd ./scripts/
./setup-redisearch.sh
```

The compiled redisearch will be in `$HOME/RediSearch`.

# How to Install RedisSearchBenchmark

1. Install Go

```
cd scripts
./install-go.sh
source ~/.bashrc
```

2. Download and compile RediSearchBenchmark

```
cd scripts
./setup-redisearchbench.sh
```

3. The executable is in `$GOPATH/bin`


# How to run start redisearch daemon?

```
cd src/pysrc
make start_redisearch_servers
```

This will start 5 servers




