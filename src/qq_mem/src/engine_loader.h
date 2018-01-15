#ifndef ENGINE_LOADER_H
#define ENGINE_LOADER_H

#include "engine_services.h"
#include "utils.h"

namespace engine_loader {
  int load_body_and_tokenized_body(SearchEngineServiceNew * engine, 
                                    const std::string &line_doc_path,
                                    const int &n_rows, 
                                    const int &col_body,
                                    const int &col_tokenized_body) {
    utils::LineDoc linedoc(line_doc_path);
    std::vector<std::string> items;
    bool has_it;

    int count = 0;
    for (int i = 0; i < n_rows; i++) {
      has_it = linedoc.GetRow(items);
      if (has_it) {
        engine->AddDocument(items[col_body], items[col_tokenized_body]);
        count++;
      } else {
        break;
      }

      if (count % 10000 == 0) {
        std::cout << "Indexed " << count << " documents" << std::endl;
      }
    }

    return count;
  }

  int load_body_and_tokenized_body_and_token_offsets(SearchEngineServiceNew * engine, 
                                    const std::string &line_doc_path,
                                    const int &n_rows, 
                                    const int &col_body,
                                    const int &col_tokenized_body,
                                    const int &col_token_offsets) {
    utils::LineDoc linedoc(line_doc_path);
    std::vector<std::string> items;
    bool has_it;

    int count = 0;
    for (int i = 0; i < n_rows; i++) {
      has_it = linedoc.GetRow(items);
      if (has_it) {
        engine->AddDocument(items[col_body], items[col_tokenized_body], items[col_token_offsets]);
        count++;
      } else {
        break;
      }

      if (count % 1000 == 0) {
        std::cout << "Indexed " << count << " documents" << std::endl;
      }
    }

    return count;
  }
} // namespace engine_loader


#endif 
