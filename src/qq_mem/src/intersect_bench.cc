#define NDEBUG

#include <iostream>
#include <cassert>

#include <glog/logging.h>

#include "intersect.h"
#include "utils.h"

typedef std::vector<int> array_t;

array_t intersect_vec(const array_t &a1, const array_t &a2) {
}

array_t findIntersection(const array_t &A, const array_t &B) {
  array_t intersection;
  int n1 = A.size();
  int n2 = B.size();
  int i = 0, j = 0;
  while (i < n1 && j < n2) {
    if (A[i] > B[j]) {
      j++;
    } else if (B[j] > A[i]) {
      i++;
    } else {
      intersection.push_back(A[i]);
      i++;
      j++;
    }
  }
  return intersection;
}


void qq_mem_bench() {
  PostingList_Vec<Posting> pl01("hello");   
  for (int i = 0; i < 1000000; i++) {
    pl01.AddPosting(Posting(i, 1, Positions{28}));
  }

  PostingList_Vec<Posting> pl02("world");   
  for (int i = 0; i < 1000000; i++) {
    pl02.AddPosting(Posting(i, 1, Positions{28}));
  }

  std::vector<const PostingList_Vec<Posting>*> lists{&pl01, &pl02};

  auto start = utils::now();
  std::vector<DocIdType> ret = intersect<Posting>(lists);
  auto end = utils::now();
  auto dur = utils::duration(start, end);

  assert(ret.size() == 1000000);
  std::cout << "QQ Intersection Took " << dur << " secs" << std::endl;
}

void leetcode_bench() {
  array_t a1, a2;
  for (int i = 0; i < 1000000; i++) {
    a1.push_back(i);
    a2.push_back(i);
  }

  auto start = utils::now();
  array_t ret2 = findIntersection(a1, a2);
  auto end = utils::now();
  auto dur = utils::duration(start, end);

  assert(ret2.size() == 1000000);
  std::cout << "LeetCode Took " << dur << " secs" << std::endl;
}


int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 

  qq_mem_bench();
  leetcode_bench();
}
