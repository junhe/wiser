#include <algorithm>
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <vector>
#include <climits>

#include "utils.h"
#include "vacuum_engine.h"

int main(int argc, char **argv) {

  TermIndex index;

  index.Load("/mnt/ssd/vacuum_engine_dump_magic/my.tip");

  auto it = index.Find("hello");
  std::cout << it->first << " : " << it->second << std::endl;

  utils::sleep(1000);  

  return 0;
}


