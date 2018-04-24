#ifndef QUERY_POOL_H
#define QUERY_POOL_H

#include <iostream>
#include <fstream>
#include <mutex>

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

   bool NextLine(std::string &line) {
     auto &ret = std::getline(in_, line);

     if (ret) {
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
  virtual int Size() = 0;
  virtual SearchQuery NextNativeQuery(const int i) = 0;
  virtual qq::SearchRequest NextGrpcQuery(const int i) = 0;
  virtual bool IsEnd() = 0;
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

  SearchQuery NextNativeQuery(const int i) override {
    query_template_[i].terms = term_pool_array_->Next(i);
    return query_template_[i];
  }

  qq::SearchRequest NextGrpcQuery(const int i) override {
    qq::SearchRequest request;

    SearchQuery query = NextNativeQuery(i);
    query.CopyTo(&request);

    return request;
  }

  // We are looping, so no end
  bool IsEnd() override {
    return false;
  }

  int Size() override {
    return term_pool_array_->Size();
  }

 private:
  std::unique_ptr<TermPoolArray> term_pool_array_;
  std::vector<SearchQuery> query_template_;
};


class QueryPoolBase {
 public:
  QueryPoolBase() {}

  void Add(const SearchQuery query) {
    pool_.push_back(query); 
  }

  bool IsEnd() const {
    return access_cnt_ == pool_.size();
  }

  int Size() const {
    return pool_.size();
  }

 protected:
  int access_cnt_ = 0;                // for iterating the query buffer
  std::vector<SearchQuery> pool_;
};


class QueryPool :public QueryPoolBase {
 public:
  const SearchQuery &Next() {
    int next = access_cnt_ % pool_.size();
    access_cnt_++;
    return pool_[next];
  }
};


class QueryPoolNoLoop :public QueryPoolBase {
 public:
  const SearchQuery &Next() {
    int next = access_cnt_;
    access_cnt_++;
    DLOG_IF(FATAL, access_cnt_ > pool_.size());
    return pool_[next];
  }
};


class QueryPoolArray {
 public:
  QueryPoolArray(const int n_pools) : array_(n_pools) {}

  const SearchQuery &Next(const int thread_i) {
    return array_[thread_i].Next();
  }

  void Add(const int thread_i, const SearchQuery query) {
    array_[thread_i].Add(query);
  }

  int Size() const {
    return array_.size();
  }

 private:
  std::vector<QueryPool> array_;
};


class QueryProducerNoLoop: public QueryProducerService {
 public:
  QueryProducerNoLoop(const std::string query_path) 
  {
    std::cout << "Loading query log from " << query_path << std::endl;
    QueryLogReader reader(query_path);
    std::string line;
    
    while(reader.NextLine(line)) {
      utils::trim(line);

      SearchQuery query(GetTerms(line));
      query.is_phrase = IsPhrase(line);

      pool_.Add(query);
    }
  }

  // pool_index is ignored in this class
  SearchQuery NextNativeQuery(const int pool_index) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return pool_.Next();
  }

  // pool_index is ignored in this class
  qq::SearchRequest NextGrpcQuery(const int pool_index) override {
    qq::SearchRequest request;

    SearchQuery query = NextNativeQuery(pool_index);
    query.CopyTo(&request);

    return request;
  }

  int Size() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return pool_.Size();
  }

  bool IsEnd() {
    std::lock_guard<std::mutex> lock(mutex_);
    return pool_.IsEnd(); 
  }

 private:
  TermList GetTerms(std::string line) {
    if (IsPhrase(line) == true) {
        line.erase(line.size() - 1, 1);
        line.erase(0, 1);
    }

    return utils::explode(line, ' ');
  }

  bool IsPhrase(const std::string line) {
    int len = line.size();
    return line.compare(0, 1, "\"") == 0 && line.compare(len - 1 , 1, "\"") == 0;
  }
 
  std::mutex mutex_;
  QueryPool pool_;
};



class QueryProducerByLog: public QueryProducerService {
 public:
  QueryProducerByLog(const std::string query_path, const int n_threads) 
    :array_(n_threads) 
  {
    std::cout << "Loading query log from " << query_path << std::endl;
    QueryLogReader reader(query_path);
    std::string line;
    
    int index = 0;
    while(reader.NextLine(line)) {
      utils::trim(line);

      SearchQuery query(GetTerms(line));
      query.is_phrase = IsPhrase(line);

      array_.Add(index % array_.Size(), query);
      index++;
    }
  }

  SearchQuery NextNativeQuery(const int pool_index) override {
    return array_.Next(pool_index);
  }

  qq::SearchRequest NextGrpcQuery(const int pool_index) override {
    qq::SearchRequest request;

    SearchQuery query = NextNativeQuery(pool_index);
    query.CopyTo(&request);

    return request;
  }

  int Size() override {
    return array_.Size();
  }

  // We are looping, so no end
  bool IsEnd() override {
    return false;
  }

 private:
  TermList GetTerms(std::string line) {
    if (IsPhrase(line) == true) {
        line.erase(line.size() - 1, 1);
        line.erase(0, 1);
    }

    return utils::explode(line, ' ');
  }

  bool IsPhrase(const std::string line) {
    int len = line.size();
    return line.compare(0, 1, "\"") == 0 && line.compare(len - 1 , 1, "\"") == 0;
  }

  QueryPoolArray array_;
};


inline std::unique_ptr<TermPoolArray> CreateTermPoolArray(const TermList &query,
    const int n_pools) {
  std::unique_ptr<TermPoolArray> array(new TermPoolArray(n_pools));
  array->LoadTerms(query);
  return array;
}


inline std::unique_ptr<QueryProducerService> CreateQueryProducer(const TermList &terms,
    const int n_pools) {
  std::unique_ptr<TermPoolArray> array(new TermPoolArray(n_pools));
  array->LoadTerms(terms);

  GeneralConfig config;
  std::unique_ptr<QueryProducerService> producer(
      new QueryProducer(std::move(array), config));

  return producer;
}

// Example setting:
//
// GeneralConfig config;
// config.SetInt("n_results", 10);
// config.SetBool("return_snippets", true);
// config.SetInt("n_snippet_passages", 3);
// config.SetBool("is_phrase", false);
inline std::unique_ptr<QueryProducerService> CreateQueryProducer(const TermList &terms,
    const int n_pools, GeneralConfig config) {
  std::unique_ptr<TermPoolArray> array(new TermPoolArray(n_pools));
  array->LoadTerms(terms);

  std::unique_ptr<QueryProducerService> producer(
      new QueryProducer(std::move(array), config));

  return producer;
}


inline std::unique_ptr<TermPoolArray> CreateTermPoolArray(
    const std::string &query_log_path, const int n_pools, const int n_queries) 
{
  std::unique_ptr<TermPoolArray> array(new TermPoolArray(n_pools));
  array->LoadFromFile(query_log_path, n_queries);
  return array;
}

#endif 
