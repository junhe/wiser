#include "query_pool.h"

void load_query_pool_from_file(QueryPool *pool, 
                     const std::string &query_log_path, 
                     const int n_queries) 
{
  QueryLogReader reader(query_log_path);
  TermList query;

  while (reader.NextQuery(query)) {
    pool->Add(query);
  }
}



// if n_queries = 0, we load the whole file
void load_query_pool_array(QueryPoolArray *array, 
                           const std::string &query_log_path, 
                           const int n_queries) {
  QueryLogReader reader(query_log_path);
  TermList query;

  int i = 0;
  while (reader.NextQuery(query)) {
    array->Add( i % array->Size(), query);
    i++;

    if (i == n_queries) {
      break;
    }
  }
}


std::unique_ptr<QueryPoolArray> create_query_pool_array(const TermList &query,
    const int n_pools) {
  std::unique_ptr<QueryPoolArray> array(new QueryPoolArray(n_pools));

  for (int i = 0; i < n_pools; i++) {
    array->Add(i, query);
  }

  return array;
}

std::unique_ptr<QueryPoolArray> create_query_pool_array(
    const std::string &query_log_path, const int n_pools, const int n_queries) 
{
  std::unique_ptr<QueryPoolArray> array(new QueryPoolArray(n_pools));
  load_query_pool_array(array.get(), query_log_path, n_queries);
  return array;
}


