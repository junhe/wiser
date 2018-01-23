#ifndef TYPES_H
#define TYPES_H


#include <string>
#include <vector>
#include <map>
#include <tuple>
#include <sstream>

#include "qq.pb.h"


// for direct IO
#define PAGE_SIZE 512

// for flash based documents
#define FLAG_DOCUMENTS_ON_FLASH false
#define DOCUMENTS_CACHE_SIZE 100
#define DOCUMENTS_ON_FLASH_FILE "/mnt/ssd/documents_store"

// for flash based postings
#define FLAG_POSTINGS_ON_FLASH false
#define POSTINGS_CACHE_SIZE 100
#define POSTINGS_ON_FLASH_FILE "/mnt/ssd/postings_store"
#define POSTING_SERIALIZATION "cereal"    // cereal or protobuf

// for precomputing of snippets generating
#define FLAG_SNIPPETS_PRECOMPUTE false

// size of snippets cache of highlighter
#define SNIPPETS_CACHE_SIZE 100
#define FLAG_SNIPPETS_CACHE false
#define SNIPPETS_CACHE_ON_FLASH_SIZE 100
#define FLAG_SNIPPETS_CACHE_ON_FLASH false
#define SNIPPETS_CACHE_ON_FLASH_FILE "/mnt/ssd/snippets_store.cache"

// Basic types for query
typedef std::string Term;
typedef std::vector<Term> TermList;
typedef TermList Query;
enum class SearchOperator {AND, OR};
typedef int DocIdType;
typedef double qq_float;

// Basic types for highlighter
typedef std::vector<int> TopDocs;
typedef std::tuple<int, int> Offset;  // (startoffset, endoffset)
typedef Offset OffsetPair; // Alias for Offset, more intuitive
typedef std::vector<Offset> Offsets;
typedef Offsets OffsetPairs; // Alias for Offsets, more intuitive
typedef std::vector<std::string> Snippets;

typedef unsigned long byte_cnt_t;

class TermWithOffset { // class Term_With_Offset
    public:
        Term term_;
        Offsets offsets_;

        TermWithOffset(Term term_in, Offsets offsets_in) : term_(term_in), offsets_(offsets_in) {} 
};
typedef std::vector<TermWithOffset> TermWithOffsetList;


enum class QueryProcessingCore {TOGETHER, BY_PHASE};
struct SearchQuery {
  SearchQuery(){}
  SearchQuery(const qq::SearchRequest &grpc_req){
    this->CopyFrom(grpc_req);
  }
  SearchQuery(const TermList &terms_in): terms(terms_in) {}
  SearchQuery(const TermList &terms_in, const bool &return_snippets_in)
    : terms(terms_in), return_snippets(return_snippets_in) {}

  TermList terms;
  int n_results = 5;
  bool return_snippets = false;
  QueryProcessingCore query_processing_core = QueryProcessingCore::TOGETHER;
  int n_snippet_passages = 3;

  std::string ToStr() const {
    std::ostringstream oss;
    for (auto &term : terms) {
      oss << term << " ";
    }
    oss << "n_results(" << n_results << ") "
        << "return_snippets(" << return_snippets << ") "
        << "n_snippet_passages(" << n_snippet_passages << ") "
        << std::endl;
    return oss.str();
  }

  void CopyTo(qq::SearchRequest *request) {
    for (auto &term : terms) {
      request->add_terms(term);
    }

    request->set_n_results(n_results);
    request->set_return_snippets(return_snippets);
    request->set_n_snippet_passages(n_snippet_passages);
    
    switch (query_processing_core) {
      case QueryProcessingCore::TOGETHER:
        request->set_query_processing_core(qq::SearchRequest_QueryProcessingCore_TOGETHER);
        break;
      case QueryProcessingCore::BY_PHASE:
        request->set_query_processing_core(qq::SearchRequest_QueryProcessingCore_BY_PHASE);
        break;
      default:
        throw std::runtime_error("query processsing core not exists");
    }
  }

  void CopyFrom(const qq::SearchRequest &grpc_req) {
    int n = grpc_req.terms_size();
    terms.clear();
    for (int i = 0; i < n; i++) {
      terms.push_back(grpc_req.terms(i));
    }

    n_results = grpc_req.n_results();
    return_snippets = grpc_req.return_snippets();
    n_snippet_passages = grpc_req.n_snippet_passages();

    switch (grpc_req.query_processing_core()) {
      case qq::SearchRequest_QueryProcessingCore_TOGETHER:
        query_processing_core = QueryProcessingCore::TOGETHER;
        break;
      case qq::SearchRequest_QueryProcessingCore_BY_PHASE:
        query_processing_core = QueryProcessingCore::BY_PHASE;
        break;
      default:
        throw std::runtime_error("query processsing core not exists");
    }
  }
};

struct SearchResultEntry {
  std::string snippet;
  DocIdType doc_id;

  std::string ToStr() {
    return "DocId: " + std::to_string(doc_id) + " Snippet: " + snippet;
  }
  void CopyTo(qq::SearchReplyEntry *grpc_entry) {
    grpc_entry->set_doc_id(doc_id);
    grpc_entry->set_snippet(snippet);
  }
};

struct SearchResult {
  std::vector<SearchResultEntry> entries;

  std::size_t Size() {return entries.size();}
  std::string ToStr() {
    std::string ret;
    for (auto entry : entries) {
      ret += entry.ToStr() + "\n";
    }
    return ret;
  }
  void CopyTo(qq::SearchReply *grpc_reply) {
    grpc_reply->clear_entries();
    for ( auto & result_entry : entries ) {
      qq::SearchReplyEntry *grpc_entry = grpc_reply->add_entries();
      result_entry.CopyTo(grpc_entry);
    }
  }
};


// Basic types for precomputation
// for snippet generating
typedef std::pair<int, float> Passage_Score; // (passage,score)
typedef std::vector<Passage_Score> Passage_Scores;
typedef std::pair<int, int> Passage_Split; // (start offset in offsets, len)
// for sentence breaking each document
typedef std::pair<int, int> Passage_Segment; // (startoffset, length)
typedef std::vector<Passage_Segment> Passage_Segments;

// Basic types for File Reader
typedef std::pair<int, int> Store_Segment;    // startoffset, length on flash based file

#endif
