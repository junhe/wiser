#include "utils.h"

#include <iostream>
#include <iomanip>
#include <sstream>
#include <thread>
#include <iterator>
#include <cassert>
#include <stdexcept>

namespace utils {

// To split a string to vector
// Not sure how good it is for unicode...
const std::vector<std::string> explode(const std::string& s, const char& c)
{
    std::string buff{""};
    std::vector<std::string> v;
    
    for(auto n:s)
    {
        if(n != c) buff+=n; else
        if(n == c && buff != "") { v.push_back(buff); buff = ""; }
    }
    if(buff != "") v.push_back(buff);
    
    return v;
}

// TODO: Replace this boost tokenizer
const std::vector<std::string> explode_by_non_letter(const std::string &text) {
    std::istringstream iss(text);
    std::vector<std::string> results((std::istream_iterator<std::string>(iss)),
                                     std::istream_iterator<std::string>());
    return results;
} 

const std::vector<std::string> explode_strict(const std::string &line, const char& sep) {
    std::string buff{""};
    std::vector<std::string> vec;
    
    for(auto ch:line)
    {
        if(ch != sep) {
            buff += ch; 
        } else { 
            vec.push_back(buff); 
            buff = "";
        }
    }
    vec.push_back(buff);
    
    return vec;
}

void sleep(int n_secs) {
  using namespace std::this_thread; // sleep_for, sleep_until
  using namespace std::chrono; // nanoseconds, system_clock, seconds

  sleep_for(seconds(n_secs));
}

const std::chrono::time_point<std::chrono::system_clock> now() {
    return std::chrono::system_clock::now();
}

const double duration(std::chrono::time_point<std::chrono::system_clock> t1, 
        std::chrono::time_point<std::chrono::system_clock> t2) {
    std::chrono::duration<double> diff = t2 - t1;
    return diff.count();
}

// Note that width is the final length of the string, not
// the number of zeros.
const std::string fill_zeros(const std::string &s, std::size_t width) {
    int n_zeros = width - s.size();
    if (n_zeros > 0) {
        return std::string(n_zeros, '0') + s; 
    } else {
        return s;
    }
}

const std::string format_double(const double &x, const int &precision) {
  std::ostringstream out;
  out << std::setprecision(precision) << x;
  return out.str();
}


std::string dump_result_table(ResultTable result_table) {
  if (result_table.size() == 0) {
    return "";
  }

  std::ostringstream oss;

  // print header
  for (auto it : result_table[0]) {
    oss << it.first << "\t\t";
  }
  oss << std::endl;

  for (auto row : result_table) {
    for (auto col : row) {
      oss << col.second << "\t\t";
    }
    oss << std::endl;
  }

  return oss.str();
}




///////////////////////////////////////////////
// LineDoc
///////////////////////////////////////////////

LineDoc::LineDoc(std::string path) {
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

bool LineDoc::GetRow(std::vector<std::string> &items) {
    std::string line;
    auto &ret = std::getline(infile_, line);

    if (ret) {
        items = explode_strict(line, '\t');
        return true;
    } else {
        return false;
    }
}





} // namespace util
