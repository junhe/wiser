#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include <glog/logging.h>

int main( int argc, char* argv[] ) {
  // global setup...
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1; // print to stderr instead of file
  // FLAGS_minloglevel = 0;  // All
  FLAGS_minloglevel = 4; // ONLY FATAL

  int result = Catch::Session().run( argc, argv );

  // global clean-up...
  return result;
}

