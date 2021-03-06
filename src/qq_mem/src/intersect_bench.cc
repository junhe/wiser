#include <iostream>
#include <cassert>

#include <glog/logging.h>

#include "posting.h"
#include "query_processing.h"
#include "utils.h"

typedef std::vector<int> array_t;

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


utils::ResultRow leetcode_bench(const int &n_postings) {
  const int n_repeats = 100;
  utils::ResultRow row;

  array_t a1, a2;
  for (int i = 0; i < n_postings; i++) {
    a1.push_back(i);
    a2.push_back(i);
  }

  auto start = utils::now();
  for (int i = 0; i < n_repeats; i++) {
    array_t ret2 = findIntersection(a1, a2);
  }
  auto end = utils::now();
  auto dur = utils::duration(start, end);

  row["duration"] = std::to_string(dur / n_repeats);
  row["n_postings"] = std::to_string(n_postings);

  return row;
}

void leetcode_bench_suite() {
  utils::ResultTable table;
  table.Append(leetcode_bench(1000));
  table.Append(leetcode_bench(10000));
  table.Append(leetcode_bench(100000));
  table.Append(leetcode_bench(1000000));

  std::cout << table.ToStr();
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 

  leetcode_bench_suite();
}
