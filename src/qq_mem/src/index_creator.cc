#include "index_creator.h"
#include "utils.h"


IndexCreator::IndexCreator(const std::string &line_doc_path, QQEngineSyncClient &client) 
    :line_doc_path_(line_doc_path), client_(client)
{}

// Use engine client to index documents in line doc
// when n_rows = -1, we fetch all rows
void IndexCreator::DoIndex(int n_rows) {
    utils::LineDoc linedoc(line_doc_path_);
    // std::cout << "line_doc_path_" << line_doc_path_ << std::endl;
    // utils::LineDoc linedoc("src/testdata/tokenized_wiki_abstract_line_doc");
    std::vector<std::string> items;
    bool has_it;
    int count = 0;
    
    while (true) {
        if (n_rows != -1 && count >= n_rows) {
          // Got n_rows in total already
          break;
        }

        has_it = linedoc.GetRow(items);
        count++;
        if (has_it) {
            // std::cout << "****************" << items.size() <<"****************" << std::endl;
            client_.AddDocument(items[0], "http://wiki", items[2]);
        } else {
            // Reached the end of the file
            break;
        }
    }
}

