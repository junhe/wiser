mkdir -p $GOPATH
cd $GOPATH
go get github.com/RedisLabs/RediSearchBenchmark
cd $GOPATH/src/github.com/RedisLabs/
rm -rf RediSearchBenchmark
git clone git@github.com:junhe/RediSearchBenchmark.git
cd RediSearchBenchmark
go install
