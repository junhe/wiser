#ifndef UTILS_H
#define UTILS_H

#include <chrono>
#include <fstream>
#include <sstream>
#include <map>
#include <string>
#include <vector>

#include "engine_services.h"

namespace utils {

const std::vector<std::string> explode(const std::string& s, const char& c);
const std::vector<std::string> explode_by_non_letter(const std::string& text);
const std::vector<std::string> explode_strict(const std::string& s, const char& c);
void sleep(int n_secs);
const std::chrono::time_point<std::chrono::system_clock> now();
const double duration(std::chrono::time_point<std::chrono::system_clock> t1, 
    std::chrono::time_point<std::chrono::system_clock> t2);
const std::string fill_zeros(const std::string &s, std::size_t width);

const std::string format_double(const double &x, const int &precision);

std::vector<DocIdType> find_top_k(const DocScoreVec &doc_scores, int k);

typedef std::map<std::string, std::string> ResultRow;
class LineDoc {
    private:
        std::ifstream infile_;
        std::vector<std::string> col_names_;

    public:
        LineDoc(std::string path);
        bool GetRow(std::vector<std::string> &items);
};

class ResultTable {
 public:
  std::vector<ResultRow> table_;

  void Append(const ResultRow &row) {
    table_.push_back(row);
  }

  std::string ToStr() {
    if (table_.size() == 0) {
      return "";
    }

    std::ostringstream oss;

    // print header
    for (auto it : table_[0]) {
      oss << it.first << "\t\t";
    }
    oss << std::endl;

    for (auto row : table_) {
      for (auto col : row) {
        oss << col.second << "\t\t";
      }
      oss << std::endl;
    }

    return oss.str();
  }
};

// Stair:
// 1           -----                                         -----
// 1
// ...         step height = number of layers in this step
// 1           -----
// 1 2
// 1 2
// ...                                                   number of steps
// 1 2
// 1 2 3
// ...
//
// 1 2 3 ..                                                  -----
class Staircase {
 private:
  const int step_height_;
  const int n_steps_;
  const int max_layer_;
  int cur_layer_;
 
 public:
  // step_height and n_steps_ must be at least 1
  Staircase(const int &step_height, const int &n_steps)
    :step_height_(step_height), n_steps_(n_steps), cur_layer_(0), 
    max_layer_(step_height * n_steps) {}

  std::string NextLayer() {
    if (cur_layer_ >= max_layer_) 
      return "";

    const int step = cur_layer_ / step_height_;
    const int width = step + 1;

    std::string str;
    for (int col = 0; col < width; col++) {
      str += std::to_string(col);
      if (col != width - 1) {
        str += " ";
      }
    }

    cur_layer_++;
    return str;
  }

  int MaxWidth() {
    // the total number of steps is equal to the max width
    return n_steps_;
  }
};

} // namespace util
#endif

