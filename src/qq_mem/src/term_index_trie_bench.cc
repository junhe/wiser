#include <algorithm>
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <vector>
#include <climits>

#include "utils.h"
#include "vacuum_engine.h"
#include "tsl/htrie_hash.h"
#include "tsl/htrie_map.h"

int main(int argc, char **argv) {

  TermIndex index;

  //Use the trie-based hashmap for TermIndex
  typedef tsl::htrie_map<char, int64_t> LocalTrieMap;
  typedef LocalTrieMap::const_iterator ConstTrieIterator;

  index.Load("/mnt/ssd/vacuum_engine_dump_magic/my.tip");

  ConstTrieIterator it = index.Find("hello");
  std::cout << it.key() << " : " << it.value() << std::endl;

  utils::sleep(1000);  

  return 0;
}


