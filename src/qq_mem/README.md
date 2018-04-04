QQ-Mem is an in-memory search engine written in C++, named after
the Chinese restaraunt near CS department of UW-Madison...
Alternatively, QQ stands for Quick Query.


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

Run QQ-mem with 1 async server and 16 sync clients

```
make grpc_benchmark
```


Run in-process QQ-mem benchmark

```
make in_proc_engine_bench_querylog
```


## How to use GLOG

Example code: 

```
#include <glog/logging.h>

int main(int argc, char* argv[]) {
 // Initialize Google's logging library.
 google::InitGoogleLogging(argv[0]);

 FLAGS_logtostderr = 1; // print to stderr instead of file
 FLAGS_stderrthreshold = 2; // print INFO and other levels above INFO (WARNING, ...)

 LOG(INFO) << "Found " << 4 << " cookies";
 LOG(WARNING) << "Found " << 4 << " cookies";
 LOG(ERROR) << "Found " << 4 << " cookies";
 <!--LOG(FATAL) << "Found " << 4 << " cookies";-->
}
```

Let's use

- INFO for debugging info
- WARNING for warning
- ERROR for error
- FATAL for fatal error, which will shut down the program


You may see log files in `/tmp/`.

More info: http://rpg.ifi.uzh.ch/docs/glog.html




