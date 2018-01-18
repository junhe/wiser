#ifndef ENGINE_LOADER_H
#define ENGINE_LOADER_H

#include "engine_services.h"

namespace engine_loader {
int load_body_and_tokenized_body(SearchEngineServiceNew * engine, 
                                  const std::string &line_doc_path,
                                  const int &n_rows, 
                                  const int &col_body,
                                  const int &col_tokenized_body);
int load_body_and_tokenized_body_and_token_offsets(SearchEngineServiceNew * engine, 
                                  const std::string &line_doc_path,
                                  const int &n_rows, 
                                  const int &col_body,
                                  const int &col_tokenized_body,
                                  const int &col_token_offsets);
 
} // namespace engine_loader


#endif 
