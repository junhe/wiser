#include "index_creator.h"
#include "utils.h"


IndexCreator::IndexCreator(const std::string &line_doc_path, QQEngineClient &client) 
    :line_doc_path_(line_doc_path), client_(client)
{}

// Use engine client to index documents in line doc
void IndexCreator::DoIndex() {
    utils::LineDoc linedoc(line_doc_path_);
    // std::cout << "line_doc_path_" << line_doc_path_ << std::endl;
    // utils::LineDoc linedoc("src/testdata/tokenized_wiki_abstract_line_doc");
    std::vector<std::string> items;
    bool has_it;
    
    while (true) {
        has_it = linedoc.GetRow(items);
        if (has_it) {
            std::cout << "****************" << items.size() <<"****************" << std::endl;
            client_.AddDocument(items[0], "http://wiki", items[2]);
        } else {
            break;
        }
    }
}

