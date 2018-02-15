#ifndef UTILS_H
#define UTILS_H

#include <chrono>
#include <fstream>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <map>
#include <unordered_map>
#include <string>
#include <vector>
#include <locale>


#include "engine_services.h"
#include "types.h"

namespace utils {

typedef std::chrono::time_point<std::chrono::system_clock> time_point;
const std::vector<std::string> explode(const std::string& s, const char& c);
const std::vector<std::string> explode_by_non_letter(const std::string& text);
const std::vector<std::string> explode_strict(const std::string& s, const char& c);
void sleep(int n_secs);
const time_point now();
const double duration(std::chrono::time_point<std::chrono::system_clock> t1, 
    std::chrono::time_point<std::chrono::system_clock> t2);
const std::string fill_zeros(const std::string &s, std::size_t width);

const std::vector<Offsets> parse_offsets(const std::string& s);
const std::string format_double(const double &x, const int &precision);

std::vector<DocIdType> find_top_k(const DocScoreVec &doc_scores, int k);
int count_terms(const std::string field);
typedef std::unordered_map<Term, int> CountMapType;
CountMapType count_tokens(const std::string &token_text);

typedef std::map<std::string, std::string> ResultRow;


void add_term_offset_entry(std::map<Term, OffsetPairs> *result, const Term &buf, const int &i);
std::map<Term, OffsetPairs> extract_offset_pairs(const std::string & token_str);
std::map<Term, OffsetPairs> construct_offset_pairs(const TermList & tokens, const std::vector<Offsets> & token_offsets);

class LineDoc {
 private:
  std::ifstream infile_;
  std::vector<std::string> col_names_;

 public:
  LineDoc(std::string path) {
    infile_.open(path);

    if (infile_.good() == false) {
      throw std::runtime_error("File may not exist");
    }

    std::string line; 
    std::getline(infile_, line);
    std::vector<std::string> items = explode(line, '#');
    // TODO: bad! this may halt the program when nothing is in line
    items = explode_by_non_letter(items.back()); 
    col_names_ = items;
  }

  bool GetRow(std::vector<std::string> &items) {
    std::string line;
    auto &ret = std::getline(infile_, line);

    if (ret) {
      items = explode_strict(line, '\t');
      return true;
    } else {
      return false;
    }
  }
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

    int width = 12;
    std::ostringstream oss;

    // print header
    for (auto it : table_[0]) {
      oss << std::setw(width) << it.first << "\t\t";
    }
    oss << std::endl;

    for (auto row : table_) {
      for (auto col : row) {
        oss << std::setw(width)  << col.second << "\t\t";
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


template<class T>
std::string format_with_commas(T value)
{
	std::stringstream ss;
	ss.imbue(std::locale(""));
	ss << std::fixed << value;
	return ss.str();
}


std::string str_qq_search_reply(const qq::SearchReply &reply);
int varint_expand_and_encode(uint32_t value, std::string *buf, const int offset);
int varint_encode(uint32_t value, std::string *buf, int offset);
int varint_decode(const std::string &buf, int offset, uint32_t *value);
int varint_decode_chars(const char *buf, const int offset, uint32_t *value);

void MapFile(std::string path, char **ret_addr, int *ret_fd, size_t *ret_file_length);
void UnmapFile(char *addr, int fd, size_t file_length);

void RemoveDir(std::string path);

} // namespace util
#endif

