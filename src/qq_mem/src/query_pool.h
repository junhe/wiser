#ifndef QUERY_POOL_H
#define QUERY_POOL_H
#include <iostream>
#include <fstream>
#include "general_config.h"

class QueryPool {

 public:
  QueryPool(const GeneralConfig &config) {
    // set source
    query_source_ = config.GetString("query_source");

    // config according to different mode
    if (query_source_ == "hardcoded") {
      query_buffer_.push_back(config.GetStringVec("terms"));
      query_buffer_size_ = 1;
    } else if (query_source_ == "querylog") {
      // read in all querys
      path_ = config.GetString("querylog_path");
      std::string str;
      std::ifstream in(path_);
      while (std::getline(in, str)) {
        query_buffer_.push_back(utils::explode(str, ' '));
      }
      query_buffer_size_ = query_buffer_.size();
      /*for (auto query: query_buffer_) {
        std::cout << "query: ";
        for (auto term: query) {
          std::cout << term << " ";
        }
        std::cout << std::endl;
      } */
    } else {
      LOG(WARNING) << "Cannot determine query source";
    }
  }
  const std::vector<std::string> & next() {
    return query_buffer_[(counter_++)%query_buffer_size_];
  }

  std::string summarize() {
    if (query_source_ == "hardcoded") {
      std::string query;
      for (auto term: query_buffer_[0]) {
        query += term + " ";
      }
      return query;
    } else if (query_source_ == "querylog") {
      return path_;
    } else {
      LOG(WARNING) << "Cannot determine query source";
    }
  }
 private:
  std::string query_source_;
  std::string path_;               // file path of query log for querylog mode
  std::vector<std::vector<std::string>> query_buffer_;
  int counter_ = 0;                // for iterating the query buffer
  int query_buffer_size_;
};

#endif 
