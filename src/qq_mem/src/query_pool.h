#ifndef QUERY_POOL_H
#define QUERY_POOL_H

#include <iostream>
#include <fstream>

#include <glog/logging.h>

#include "qq.pb.h"
#include "general_config.h"
#include "types.h"
#include "utils.h"

class QueryLogReader {
 public:
   QueryLogReader(const std::string &path)
    :in_(path) {
      if (in_.good() == false) {
        throw std::runtime_error("File may not exist");
      }
   }

   bool NextQuery(TermList &query) {
     std::string line;
     auto &ret = std::getline(in_, line);

     if (ret) {
       query = utils::explode(line, ' ');
       return true;
     } else {
       return false;
     }
   }

 private:
   std::ifstream in_;
};


class TermPool {
 public:
  TermPool() {}

  void LoadFromFile(const std::string &query_log_path, const int n_queries) {
    QueryLogReader reader(query_log_path);
    TermList query;

    while (reader.NextQuery(query)) {
      Add(query);
    }
  }

  const TermList &Next() {
    return query_buffer_[(counter_++) % query_buffer_size_];
  }

  void Add(const TermList &query) {
    query_buffer_.push_back(query);
    query_buffer_size_ = query_buffer_.size();
  }

 private:
  std::vector<TermList> query_buffer_;
  int counter_ = 0;                // for iterating the query buffer
  int query_buffer_size_;
};


class TermPoolArray {
 public:
  TermPoolArray(const int n_pools): array_(n_pools) {
  }

  void LoadTerms(const TermList &query) {
    for (int i = 0; i < array_.size(); i++) {
      Add(i, query);
    }
  }

  void LoadFromFile(const std::string &query_log_path, const int n_queries) {
    QueryLogReader reader(query_log_path);
    TermList query;

    int i = 0;
    while (reader.NextQuery(query)) {
      Add( i % array_.size(), query);
      i++;

      if (i == n_queries) {
        break;
      }
    }
  }

  void Add(const int i, const TermList &query) {
    array_[i].Add(query);
  }

  const TermList &Next(const int i) {
    return array_[i].Next();
  }
  
  const int Size() {
    return array_.size();
  }

  TermPool *GetPool(const int i) {
    return &array_[i];
  }

 private:
  std::vector<TermPool> array_;
};


class QueryProducerService {
 public:
  virtual SearchQuery NextNativeQuery(const int i) = 0;
  virtual qq::SearchRequest NextGrpcQuery(const int i) = 0;
};


// It is a wrapper of TermPoolArray. This class will
// return full Queries, instead of just terms
class QueryProducer: public QueryProducerService {
 public:
  QueryProducer(std::unique_ptr<TermPoolArray> term_pool_array, 
      GeneralConfig config) 
      :term_pool_array_(std::move(term_pool_array)),
       query_template_(term_pool_array_->Size())
  {
    for (auto &query : query_template_) {
      query.n_results = config.HasKey("n_results")? 
        config.GetInt("n_results") : 5;
      query.return_snippets = config.HasKey("return_snippets")? 
        config.GetBool("return_snippets") : true;
      query.n_snippet_passages = config.HasKey("n_snippet_passages")? 
        config.GetInt("n_snippet_passages") : 3;
      query.is_phrase = config.HasKey("is_phrase")? 
        config.GetBool("is_phrase") : false;
    }
  }

  SearchQuery NextNativeQuery(const int i) {
    query_template_[i].terms = term_pool_array_->Next(i);
    return query_template_[i];
  }

  qq::SearchRequest NextGrpcQuery(const int i) {
    qq::SearchRequest request;

    SearchQuery query = NextNativeQuery(i);
    query.CopyTo(&request);

    return request;
  }

  int Size() {
    return term_pool_array_->Size();
  }

 private:
  std::unique_ptr<TermPoolArray> term_pool_array_;
  std::vector<SearchQuery> query_template_;
};


std::unique_ptr<TermPoolArray> CreateTermPoolArray(const TermList &terms,
    const int n_pools);
std::unique_ptr<TermPoolArray> CreateTermPoolArray(
    const std::string &query_log_path, const int n_pools, const int n_queries=0);
std::unique_ptr<QueryProducer> CreateQueryProducer(const TermList &terms,
    const int n_pools);
std::unique_ptr<QueryProducer> CreateQueryProducer(const TermList &terms,
    const int n_pools, GeneralConfig config);
#endif 
