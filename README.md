# flashsearch


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
cd $GOPATH
go get github.com/RedisLabs/RediSearchBenchmark
```

3. The executable is in `$GOPATH/bin`


# How to run start redisearch daemon?

```
cd src/pysrc
make start_redisearch_servers
```

This will start 5 servers




