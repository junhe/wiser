#ifndef ENGINE_LOADER_H
#define ENGINE_LOADER_H

#include <climits>

#include "engine_services.h"
#include "utils.h"


class LineDocParserService {
 public:
  LineDocParserService(const std::string line_doc_path)
    :linedoc_(line_doc_path), n_rows_(INT_MAX) {
  }

  LineDocParserService(const std::string line_doc_path, const int n_rows)
    :linedoc_(line_doc_path), n_rows_(n_rows) {
  }

  // if the returned bool is false, either the 
  bool Pop(DocInfo *doc_info) {
    if (next_index_ >= n_rows_) {
      return false;
    }

    std::vector<std::string> items;
    bool has_it;

    has_it = linedoc_.GetRow(items);

    if (has_it) {
      *doc_info = ParseLine(items);
      next_index_++;
      return true;
    } else {
      return false;
    }
  }

  int Count() {
    return next_index_;
  }

 protected:
  utils::LineDoc linedoc_;
  int next_index_ = 0;
  const int n_rows_;

  virtual DocInfo ParseLine(std::vector<std::string> items) = 0;
};


// body and token
class LineDocParserToken : public LineDocParserService {
 public:
  LineDocParserToken(const std::string line_doc_path) 
    :LineDocParserService(line_doc_path) {}

  LineDocParserToken(const std::string line_doc_path, const int n_rows) 
    :LineDocParserService(line_doc_path, n_rows) {}

 protected:
  DocInfo ParseLine(std::vector<std::string> items) {
    return DocInfo(items[2], items[2], "", "", "TOKEN_ONLY");
  }
};

// body, token, offset
class LineDocParserOffset : public LineDocParserService {
 public:
  LineDocParserOffset(const std::string line_doc_path) 
    :LineDocParserService(line_doc_path) {}

  LineDocParserOffset(const std::string line_doc_path, const int n_rows) 
    :LineDocParserService(line_doc_path, n_rows) {}

 protected:
  DocInfo ParseLine(std::vector<std::string> items) {
    return DocInfo(items[1], items[2], items[3], "", "WITH_OFFSETS");
  }
};

// body, token, offset, position
class LineDocParserPosition : public LineDocParserService {
 public:
  LineDocParserPosition(const std::string line_doc_path) 
    :LineDocParserService(line_doc_path) {}

  LineDocParserPosition(const std::string line_doc_path, const int n_rows) 
    :LineDocParserService(line_doc_path, n_rows) {}

 protected:
  DocInfo ParseLine(std::vector<std::string> items) {
    return DocInfo(items[1], items[2], items[3], items[4], "WITH_POSITIONS");
  }
};


class LineDocParserPhraseEnd : public LineDocParserService {
 public:
  LineDocParserPhraseEnd(const std::string line_doc_path) 
    :LineDocParserService(line_doc_path) {}

  LineDocParserPhraseEnd(const std::string line_doc_path, const int n_rows) 
    :LineDocParserService(line_doc_path, n_rows) {}

 protected:
  DocInfo ParseLine(std::vector<std::string> items) {
    return DocInfo(items[1], items[2], items[3], items[4], items[5], 
        "WITH_PHRASE_END");
  }
};


class LineDocParserBiBloom : public LineDocParserService {
 public:
  LineDocParserBiBloom(const std::string line_doc_path) 
    :LineDocParserService(line_doc_path) {}

  LineDocParserBiBloom(const std::string line_doc_path, const int n_rows) 
    :LineDocParserService(line_doc_path, n_rows) {}

 protected:
  DocInfo ParseLine(std::vector<std::string> items) {
    return DocInfo(items[1], items[2], items[3], items[4], items[6], items[5], 
        "WITH_BI_BLOOM");
  }
};




#endif 
