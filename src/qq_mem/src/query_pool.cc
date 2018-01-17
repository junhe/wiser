#include "query_pool.h"



void load_query_pool(QueryPool *pool, const GeneralConfig &config) {
  auto query_source_ = config.GetString("query_source");

  // config according to different mode
  if (query_source_ == "hardcoded") {
    pool->Add(config.GetStringVec("terms"));
  } else if (query_source_ == "querylog") {
    // read in all querys
    QueryLogReader reader(config.GetString("querylog_path"));
    TermList query;
    while (reader.NextQuery(query)) {
      pool->Add(query);
    }
  } else {
    LOG(WARNING) << "Cannot determine query source";
  }
}

void load_query_pool_array(QueryPoolArray *array, 
                           const std::string &query_log_path) {
  QueryLogReader reader(query_log_path);
  TermList query;

  int i = 0;
  while (reader.NextQuery(query)) {
    array->Add( i % array->Size(), query);
    i++;
  }
}


