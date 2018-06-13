#ifndef TYPES_H
#define TYPES_H


#include <string>
#include <vector>
#include <map>
#include <tuple>
#include <sstream>
#include <sstream>
#include <math.h>

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


#define COMPRESSED_DOC_MAGIC 0x33

#define SKIP_LIST_FIRST_BYTE 0xA3
#define BLOOM_SKIP_LIST_FIRST_BYTE 0xA4
#define POSTING_LIST_FIRST_BYTE 0xF4
#define PACK_FIRST_BYTE 0xD6
#define VINTS_FIRST_BYTE 0x9B
#define BLOOM_BOX_FIRST_BYTE 0xF5
#define VACUUM_FIRST_BYTE 0x88

const int BLM_NOT_PRESENT = 0;
const int BLM_MAY_PRESENT = 1;
const int BLM_NOT_INITIALIZED = -1;


const off_t KB = 1024;
const off_t MB = 1048576;
const off_t GB = 1073741824L;


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
typedef int Position;
typedef std::vector<Position> Positions;
typedef std::vector<std::string> Snippets;

typedef unsigned long byte_cnt_t;

class TermWithOffset { // class Term_With_Offset
    public:
        Term term_;
        Offsets offsets_;

        TermWithOffset(Term term_in, Offsets offsets_in) : term_(term_in), offsets_(offsets_in) {} 
};
typedef std::vector<TermWithOffset> TermWithOffsetList;


class DocInfo {
 public:
  DocInfo() {}

  DocInfo(const std::string &body, const std::string &tokens, 
      const std::string &token_offsets, const std::string &token_positions, 
      const std::string &format) 
    :body_(body), tokens_(tokens), token_offsets_(token_offsets), 
     token_positions_(token_positions), format_(format) {}

  DocInfo(const std::string &body, const std::string &tokens, 
      const std::string &token_offsets, const std::string &token_positions, 
      const std::string &phrase_ends, const std::string &format) 
    :body_(body), tokens_(tokens), token_offsets_(token_offsets), 
     token_positions_(token_positions), phrase_ends_(phrase_ends), 
     format_(format) {}

  DocInfo(const std::string &body, const std::string &tokens, 
      const std::string &token_offsets, const std::string &token_positions, 
      const std::string &phrase_begins, const std::string &phrase_ends, 
      const std::string &format) 
    :body_(body), tokens_(tokens), token_offsets_(token_offsets), 
     token_positions_(token_positions), phrase_begins_(phrase_begins),
     phrase_ends_(phrase_ends), 
     format_(format) {}


  TermList GetTokens() const;

  // return a table of offset pairs
  // Each row is for a term
  std::vector<OffsetPairs> GetOffsetPairsVec() const;

  // return a table of positions
  // Each row is for a term
  std::vector<Positions> GetPositions() const;

  std::vector<Term> ParsePhraseElems(const std::string &s) const;
  std::vector<Term> GetPhraseBegins() const;

  std::vector<Term> GetPhraseEnds() const;

  const std::string &Body() const {return body_;}

  const std::string &Tokens() const {return tokens_;}

  const std::string &TokenOffsets() const {return token_offsets_;}

  const std::string &TokenPositions() const {return token_positions_;}

  const std::string &PhraseBegins() const {return phrase_begins_;}

  const std::string &PhraseEnds() const {return phrase_ends_;}
  
  const std::string &Format() const {return format_;}

  const int BodyLength() const;

  std::string ToStr() const {
    return "body: " + body_ + 
           "\ntokens: " + tokens_ +
           "\noffsets: " + token_offsets_ + 
           "\noffsets (parsed): " + ToOffsetsStr() + 
           "\npositions: " + token_positions_ + 
           "\npositions (parsed): " + ToPositionStr() + 
           "\nformat: " + format_ + 
           "\n";
  }

  std::string ToOffsetsStr() const {
    auto table = GetOffsetPairsVec();
    std::string ret;

    for (auto &row : table) {
      for (auto &pair : row) {
        ret += "(";
        ret += std::to_string(std::get<0>(pair));
        ret += ",";
        ret += std::to_string(std::get<1>(pair));
        ret += "),";
      }
      ret += "|| ";
    }
    return ret;
  }

  std::string ToPositionStr() const {
    auto table = GetPositions(); 
    std::string ret;
    for (auto &row : table) {
      for (auto &cell : row) {
        ret += std::to_string(cell) + ",";
      }
      ret += "|| ";
    }
    return ret;
  }

 private:
  std::string body_;
  std::string tokens_;
  std::string token_offsets_;
  std::string token_positions_;
  std::string phrase_begins_;
  std::string phrase_ends_;
  std::string format_;
};


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
  int n_snippet_passages = 3;
  bool is_phrase = false;

  std::string ToStr() const {
    std::ostringstream oss;
    for (auto &term : terms) {
      oss << term << " ";
    }
    oss << "n_results(" << n_results << ") "
        << "return_snippets(" << return_snippets << ") "
        << "n_snippet_passages(" << n_snippet_passages << ") "
        << "is_phrase(" << is_phrase << ") "
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
    request->set_is_phrase(is_phrase);
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
    is_phrase = grpc_req.is_phrase();
  }
};


struct SearchResultEntry {
  std::string snippet;
  DocIdType doc_id;
  qq_float doc_score;

  std::string ToStr() {
    return "DocId: " + std::to_string(doc_id) 
      + " Doc Score: " + std::to_string(doc_score)
      + " Snippet: \"" + snippet + "\"";
  }

  void CopyTo(qq::SearchReplyEntry *grpc_entry) {
    grpc_entry->set_doc_id(doc_id);
    grpc_entry->set_snippet(snippet);
    grpc_entry->set_doc_score(doc_score);
  }

  friend bool operator == (const SearchResultEntry &a, const SearchResultEntry &b) {
    if (a.snippet != b.snippet) {
      std::cout << "snippet: " << a.snippet << " != " << b.snippet << std::endl;
      return false;
    }

    if (a.doc_id != b.doc_id) {
      std::cout << "doc id: " << a.doc_id << " != " << b.doc_id << std::endl;
      return false;
    }

    if (abs(a.doc_score - b.doc_score) > 0.001) {
      std::cout << "doc score: "<< a.doc_score << " != " << b.doc_score << std::endl;
      return false;
    }

    return true;
  }

  friend bool operator != (const SearchResultEntry &a, const SearchResultEntry &b) {
    return !(a == b);
  }
};


struct SearchResult {
  std::vector<SearchResultEntry> entries;
  std::vector<int> doc_freqs; // doc freq of terms queried

  const SearchResultEntry &operator [](int i) const {
    return entries[i];
  }

  friend bool operator == (const SearchResult &a, const SearchResult &b) {
    if (a.Size() != b.Size()) {
      std::cout << "Num Entries: "<< a.Size() << " != " << b.Size() << std::endl;
      return false;
    }

    for (std::size_t i = 0; i < a.Size(); i++) {
      if (a[i] != b[i])  {
        std::cout << "entry not equal: " << i << std::endl;
        return false;
      }
    }

    return true;
  }

  friend bool operator != (const SearchResult &a, const SearchResult &b) {
    return !(a == b);
  }

  std::size_t Size() const {return entries.size();}

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
