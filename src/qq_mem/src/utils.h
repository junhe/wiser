#ifndef UTILS_H
#define UTILS_H

#include <chrono>
#include <fstream>
#include <sstream>
#include <map>
#include <string>
#include <vector>

#include "engine_services.h"
#include "types.h"

namespace utils {

const std::vector<std::string> explode(const std::string& s, const char& c);
const std::vector<std::string> explode_by_non_letter(const std::string& text);
const std::vector<std::string> explode_strict(const std::string& s, const char& c);
void sleep(int n_secs);
const std::chrono::time_point<std::chrono::system_clock> now();
const double duration(std::chrono::time_point<std::chrono::system_clock> t1, 
    std::chrono::time_point<std::chrono::system_clock> t2);
const std::string fill_zeros(const std::string &s, std::size_t width);

const std::vector<Offsets> parse_offsets(const std::string& s);
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
  const int step_width_;
  const int n_steps_;
  const int max_layer_;
  int cur_layer_;
  std::string layer_string_;
 
 public:
  // step_height and n_steps_ must be at least 1
  Staircase(const int &step_height, const int &step_width, const int &n_steps)
    :step_height_(step_height), step_width_(step_width), n_steps_(n_steps), cur_layer_(0), 
    max_layer_(step_height * n_steps) {}

  std::string NextLayer() {
    if (cur_layer_ >= max_layer_) 
      return "";

    const int step = cur_layer_ / step_height_;
    const int width = (step + 1) * step_width_;

    if (cur_layer_ % step_height_ == 0) {
      if (cur_layer_ != 0) {
        layer_string_ += " ";
      }
      int end = (step + 1) * step_width_;
      for (int col = step * step_width_; col < end; col++) {
        layer_string_ += std::to_string(col);
        if (col < end - 1) {
          layer_string_ += " ";
        }
      }
    }

    cur_layer_++;
    return layer_string_;
  }

  int MaxWidth() {
    return n_steps_ * step_width_;
  }
};

} // namespace util
#endif

