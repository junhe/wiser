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



} // namespace util
#endif

