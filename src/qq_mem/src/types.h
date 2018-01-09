#ifndef TYPES_H
#define TYPES_H


#include <string>
#include <vector>
#include <map>
#include <tuple>

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

class TermWithOffset { // class Term_With_Offset
    public:
        Term term_;
        Offsets offsets_;

        TermWithOffset(Term term_in, Offsets offsets_in) : term_(term_in), offsets_(offsets_in) {} 
};
typedef std::vector<TermWithOffset> TermWithOffsetList;


struct SearchQuery {
  SearchQuery(const TermList &terms_in): terms(terms_in) {}

  TermList terms;
  SearchOperator op = SearchOperator::AND;
  int n_results = 10;
  bool return_snippets = false;
};

struct SearchResultEntry {
  std::string snippet;
  DocIdType doc_id;
};

struct SearchResult {
  std::vector<SearchResultEntry> entries;
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
