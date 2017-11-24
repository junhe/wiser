#include "index_creator.h"


IndexCreator::IndexCreator(const std::string &line_doc_path, QQEngineClient &client) 
    :line_doc_path_(line_doc_path_), client_(client)
{}

// Use engine client to index documents in line doc
void IndexCreator::DoIndex() {
    utils::LineDoc linedoc(line_doc_path_);
    std::vector<std::string> items;

    for (auto s : items) {
    }
}

