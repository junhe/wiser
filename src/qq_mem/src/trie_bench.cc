#include <algorithm>
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <vector>
#include <climits>


#include <gperftools/profiler.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "vacuum_engine.h"


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_minloglevel = 3; 

	gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::cout << "hello" << std::endl;
}


