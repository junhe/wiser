#include "query_pool.h"


std::unique_ptr<TermPoolArray> CreateTermPoolArray(const TermList &query,
    const int n_pools) {
  std::unique_ptr<TermPoolArray> array(new TermPoolArray(n_pools));
  array->LoadTerms(query);
  return array;
}


std::unique_ptr<QueryProducerService> CreateQueryProducer(const TermList &terms,
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
std::unique_ptr<QueryProducerService> CreateQueryProducer(const TermList &terms,
    const int n_pools, GeneralConfig config) {
  std::unique_ptr<TermPoolArray> array(new TermPoolArray(n_pools));
  array->LoadTerms(terms);

  std::unique_ptr<QueryProducerService> producer(
      new QueryProducer(std::move(array), config));

  return producer;
}


std::unique_ptr<TermPoolArray> CreateTermPoolArray(
    const std::string &query_log_path, const int n_pools, const int n_queries) 
{
  std::unique_ptr<TermPoolArray> array(new TermPoolArray(n_pools));
  array->LoadFromFile(query_log_path, n_queries);
  return array;
}


