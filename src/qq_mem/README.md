QQ-Mem is an in-memory search engine written in C++, named after
a Chinese restaraunt near the CS department at UW-Madison...
Alternatively, QQ stands for Quick Query.

(QQ-Mem has been renamed to Vacuum)


Please contact Jun He (jhe@cs.wisc.edu) if you have any questions.

## Build

To build, you need to run the following command to install
grpc, protobuf.

```
./setup.sh
```

Also, make sure you have cmake 3.2 or above.

Then you can finally build by 

```
./build.sh
```

This command will put compiled files to `./build/`.

The initial grpc files are from https://github.com/IvanSafonov/grpc-cmake-example

## Generate index

See `tools/indexer.py` for details. This file includes scripts to create index for Vacuum. 

The input of indexing is in the linedoc format. You may find the format by reading the consumer C++ code. 

In `tools/indexer.py`, we build and invoke `create_qq_mem_dump` to convert linedoc to an old format that we had. Then we build and invoke `convert_qq_to_vacuum` to convert the old format to the new format that the lastest version of Vacuum can load.

## Test

Simply run:

```
make test_fast
```

or

```
make test
```

## Run benchmark

Run Vacuum with 1 async server and 16 sync clients

```
make grpc_benchmark
```


Run in-process Vacuum benchmark

```
make in_proc_engine_bench_querylog
```

