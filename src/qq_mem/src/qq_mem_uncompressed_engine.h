#ifndef QQ_MEM_UNCOMPRESSED_ENGINE_H
#define QQ_MEM_UNCOMPRESSED_ENGINE_H

#include <assert.h>

#include "doc_length_store.h"
#include "intersect.h"
#include "utils.h"
#include "native_doc_store.h"
#include "unifiedhighlighter.h"
#include "engine_loader.h"
#include "posting_list_delta.h"


class InvertedIndexQqMem {
 private:
  typedef PostingList_Vec<RankingPostingWithOffsets> PostingListType;
  typedef std::unordered_map<Term, PostingListType> IndexStore;
  IndexStore index_;

 public:
  typedef IndexStore::const_iterator const_iterator;
  typedef std::vector<const PostingList_Vec<RankingPostingWithOffsets>*> PlPointers;

  PlPointers FindPostinglists(const TermList &terms) {
    PlPointers postinglist_pointers;

    for (auto term : terms) {
      auto it = index_.find(term);
      if (it == index_.end()) {
        break;
      }

      postinglist_pointers.push_back(&it->second);
    }

    return postinglist_pointers;
  }

  std::map<std::string, int> PostinglistSizes(const TermList &terms) {
    std::map<std::string, int> ret;

    auto pointers = FindPostinglists(terms);
    for (auto p : pointers) {
      ret[p->GetTerm()] = p->Size();
    }

    return ret;
  }

  void AddDocument(const int &doc_id, const std::string &body, 
      const std::string &tokens) {
    utils::CountMapType token_counts = utils::count_tokens(tokens);
    std::map<Term, OffsetPairs> term_offsets = utils::extract_offset_pairs(tokens);

    for (auto token_it = token_counts.begin(); token_it != token_counts.end(); 
        token_it++) {
      IndexStore::iterator it;
      auto term = token_it->first;
      it = index_.find(term);

      if (it == index_.cend()) {
        // term does not exist
        std::pair<IndexStore::iterator, bool> ret;
        ret = index_.insert( std::make_pair(term, PostingListType(term)) );
        it = ret.first;
      } 

      PostingListType &postinglist = it->second;        
      postinglist.AddPosting(
          RankingPostingWithOffsets(doc_id, token_it->second, term_offsets[term]));
    }
  }

  void AddDocument(const int &doc_id, const std::string &body, 
      const std::string &tokens, const std::string &token_offsets) {
    TermList token_vec = utils::explode(tokens, ' ');
    std::vector<Offsets> offsets_parsed = utils::parse_offsets(token_offsets);
    
    assert(token_vec.size() == offsets_parsed.size());

    //std::cout << "Add document " << doc_id << ": " << token_vec.size() << " : ";
    for (int i = 0; i < token_vec.size(); i++) {
      IndexStore::iterator it = index_.find(token_vec[i]);

      if (it == index_.cend()) {
        // term does not exist
        std::pair<IndexStore::iterator, bool> ret;
        ret = index_.insert( std::make_pair(token_vec[i], PostingListType(token_vec[i])) );
        it = ret.first;
      } 
      
      PostingListType &postinglist = it->second;
      //std::cout << i << ", " << token_vec[i];
      postinglist.AddPosting(
          RankingPostingWithOffsets(doc_id, offsets_parsed[i].size(), offsets_parsed[i]));
      //std::cout << ";" ;
    }
  }

  // terms must have unique terms.
  IntersectionResult FindIntersection(const TermList &terms) {
    PlPointers postinglist_pointers = FindPostinglists(terms);
    IntersectionResult result;
    if (postinglist_pointers.size() < terms.size()) {
      return result; // return an empty one
    }

    intersect<RankingPostingWithOffsets>(postinglist_pointers, &result);
    return result;
  }

  std::vector<DocIdType> Search(const TermList &terms, const SearchOperator &op) {
    if (op != SearchOperator::AND) {
        throw std::runtime_error("NotImplementedError");
    }

    std::vector<DocIdType> doc_ids;
    PlPointers postinglist_pointers = FindPostinglists(terms);

    return intersect<RankingPostingWithOffsets>(postinglist_pointers, nullptr);
  }

  // Return number of posting lists
  std::size_t Size() const {
    return index_.size();
  }
};

class InvertedIndexQqMemDelta {
 private:
  typedef PostingListDelta PostingListType;
  typedef std::unordered_map<Term, PostingListType> IndexStore;
  IndexStore index_;

 public:
  typedef IndexStore::const_iterator const_iterator;
  typedef std::vector<const PostingListType*> PlPointers;

  PlPointers FindPostinglists(const TermList &terms) {
    PlPointers postinglist_pointers;

    for (auto term : terms) {
      auto it = index_.find(term);
      if (it == index_.end()) {
        break;
      }

      postinglist_pointers.push_back(&it->second);
    }

    return postinglist_pointers;
  }

  std::map<std::string, int> PostinglistSizes(const TermList &terms) {
    std::map<std::string, int> ret;

    auto pointers = FindPostinglists(terms);
    for (int i = 0; i < terms.size(); i++) {
      ret[terms[i]] = pointers[i]->Size();
    }

    return ret;
  }

  void AddDocument(const int &doc_id, const std::string &body, 
      const std::string &tokens) {
    utils::CountMapType token_counts = utils::count_tokens(tokens);
    std::map<Term, OffsetPairs> term_offsets = utils::extract_offset_pairs(tokens);

    for (auto token_it = token_counts.begin(); token_it != token_counts.end(); 
        token_it++) {
      IndexStore::iterator it;
      auto term = token_it->first;
      it = index_.find(term);

      if (it == index_.cend()) {
        // term does not exist
        std::pair<IndexStore::iterator, bool> ret;
        ret = index_.insert( std::make_pair(term, PostingListType(term)) );
        it = ret.first;
      } 

      PostingListType &postinglist = it->second;        
      postinglist.AddPosting(
          RankingPostingWithOffsets(doc_id, token_it->second, term_offsets[term]));
    }
  }

  void AddDocument(const int &doc_id, const std::string &body, 
      const std::string &tokens, const std::string &token_offsets) {
    TermList token_vec = utils::explode(tokens, ' ');
    std::vector<Offsets> offsets_parsed = utils::parse_offsets(token_offsets);
    
    assert(token_vec.size() == offsets_parsed.size());

    //std::cout << "Add document " << doc_id << ": " << token_vec.size() << " : ";
    for (int i = 0; i < token_vec.size(); i++) {
      IndexStore::iterator it = index_.find(token_vec[i]);

      if (it == index_.cend()) {
        // term does not exist
        std::pair<IndexStore::iterator, bool> ret;
        ret = index_.insert( std::make_pair(token_vec[i], PostingListType(token_vec[i])) );
        it = ret.first;
      } 
      
      PostingListType &postinglist = it->second;
      //std::cout << i << ", " << token_vec[i];
      postinglist.AddPosting(
          RankingPostingWithOffsets(doc_id, offsets_parsed[i].size(), offsets_parsed[i]));
      //std::cout << ";" ;
    }
  }

  IntersectionResult FindIntersection(const TermList &terms) {
    LOG(FATAL) << "FindIntersection not implemented in this inverted index" << std::endl;
  }

  // Return number of posting lists
  std::size_t Size() const {
    return index_.size();
  }
};



class QqMemUncompressedEngine : public SearchEngineServiceNew {
 private:
  int next_doc_id_ = 0;
  SimpleDocStore doc_store_;
  InvertedIndexQqMem inverted_index_;
  DocLengthStore doc_lengths_;
  SimpleHighlighter highlighter_;

  int NextDocId() {
    return next_doc_id_++;
  }

 public:
  // colum 2 should be tokens
  int LoadLocalDocuments(const std::string &line_doc_path, 
      int n_rows, const std::string loader) {
    int ret;
    if (loader == "naive") {
      ret = engine_loader::load_body_and_tokenized_body(
         this, line_doc_path, n_rows, 2, 2);
    } else if (loader == "with-offsets") {
      ret = engine_loader::load_body_and_tokenized_body_and_token_offsets(
          this, line_doc_path, n_rows, 1, 2, 3);
    } else {
      throw std::runtime_error("Loader " + loader + " is not supported");
    }

    LOG(WARNING) << "Number of terms in inverted index: " << TermCount();
    return ret;
  }

  void AddDocument(const std::string &body, const std::string &tokenized_body) {
    AddDocumentReturnId(body, tokenized_body);
  }

  void AddDocument(const std::string &body, const std::string &tokenized_body, const std::string &token_offsets) {
    AddDocumentReturnId(body, tokenized_body, token_offsets);
  }

  DocIdType AddDocumentReturnId(const std::string &body, const std::string &tokens) {
    int doc_id = NextDocId();

    doc_store_.Add(doc_id, body);
    inverted_index_.AddDocument(doc_id, body, tokens);
    doc_lengths_.AddLength(doc_id, utils::count_terms(body));
    return doc_id;
  }

  DocIdType AddDocumentReturnId(const std::string &body, const std::string &tokens, const std::string &token_offsets) {
    int doc_id = NextDocId();

    doc_store_.Add(doc_id, body);
    inverted_index_.AddDocument(doc_id, body, tokens, token_offsets);
    doc_lengths_.AddLength(doc_id, utils::count_terms(body)); // TODO modify to count on offsets?
    return doc_id;
  }
  
  std::string GetDocument(const DocIdType &doc_id) {
    return doc_store_.Get(doc_id);
  }

  int TermCount() const {
    return inverted_index_.Size();
  }

  std::map<std::string, int> PostinglistSizes(const TermList &terms) {
    return inverted_index_.PostinglistSizes(terms);
  }

  int GetDocLength(const DocIdType &doc_id) {
    return doc_lengths_.GetLength(doc_id);
  }

  IntersectionResult Query(const TermList &terms) {
    return inverted_index_.FindIntersection(terms);
  }

  DocScoreVec Score(const IntersectionResult &result) {
    return score_docs(result, doc_lengths_);
  }

  std::vector<DocIdType> FindTopK(const DocScoreVec &doc_scores, int k) {
    return utils::find_top_k(doc_scores, k);
  }

  SearchResult ProcessQueryTogether(const SearchQuery &query) {
    SearchResult result;

    if (query.n_results == 0) {
      return result;
    }

    InvertedIndexQqMem::PlPointers lists = 
      inverted_index_.FindPostinglists(query.terms);

    if (lists.size() == 0) {
      return result;
    }

    QueryProcessor<RankingPostingWithOffsets> processor(lists, doc_lengths_, doc_lengths_.Size(), 
                             query.n_results);  
    auto top_k = processor.Process();

    for (auto & top_doc_entry : top_k) {
      SearchResultEntry result_entry;
      result_entry.doc_id = top_doc_entry.doc_id;

      if (query.return_snippets == true) {
        result_entry.snippet = GenerateSnippet(top_doc_entry.doc_id,
            top_doc_entry.postings, query.n_snippet_passages);
      }

      result.entries.push_back(result_entry);
    }

    return result;
  }

  std::string GenerateSnippet(const DocIdType &doc_id, 
      const std::vector<RankingPostingWithOffsets> &postings, 
      const int n_passages) {
    OffsetsEnums res = {};

    for (int i = 0; i < postings.size(); i++) {
      res.push_back(Offset_Iterator(*postings[i].GetOffsetPairs()));
    }

    return highlighter_.highlightOffsetsEnums(res, n_passages, doc_store_.Get(doc_id));
  }

  SearchResult Search(const SearchQuery &query) {
    if (query.query_processing_core == QueryProcessingCore::TOGETHER) {
      return ProcessQueryTogether(query);
    } else if (query.query_processing_core == QueryProcessingCore::BY_PHASE) {
      return ProcessQueryStepByStep(query);
    }
  }

  SearchResult ProcessQueryStepByStep(const SearchQuery &query) {
    SearchResult result;

    auto intersection_result = Query(query.terms);
    auto scores = Score(intersection_result);
    std::vector<DocIdType> top_k = FindTopK(scores, query.n_results);

    for (auto doc_id : top_k) {
      SearchResultEntry entry;
      entry.doc_id = doc_id;

      if (query.return_snippets == true) {
        auto row = intersection_result.GetRow(doc_id);
        entry.snippet = GenerateSnippet(doc_id, row, query.n_snippet_passages);
      }

      result.entries.push_back(entry);
    }

    return result;
  }

  std::string GenerateSnippet(const DocIdType &doc_id, 
                              const IntersectionResult::row_dict_t *row,
                              const int n_passages) {
    OffsetsEnums res = {};
    
    for (auto col_it = row->cbegin() ; col_it != row->cend(); col_it++) {
      auto p_posting = IntersectionResult::GetPosting(col_it);
      res.push_back(Offset_Iterator(*p_posting->GetOffsetPairs()));
    }

    return highlighter_.highlightOffsetsEnums(res,  n_passages, doc_store_.Get(doc_id));
  }
};


class QqMemUncompressedEngineDelta : public SearchEngineServiceNew {
 private:
  int next_doc_id_ = 0;
  SimpleDocStore doc_store_;
  InvertedIndexQqMemDelta inverted_index_;
  DocLengthStore doc_lengths_;
  SimpleHighlighter highlighter_;

  int NextDocId() {
    return next_doc_id_++;
  }

 public:
  // colum 2 should be tokens
  int LoadLocalDocuments(const std::string &line_doc_path, 
      int n_rows, const std::string loader) {
    int ret;
    if (loader == "naive") {
      ret = engine_loader::load_body_and_tokenized_body(
         this, line_doc_path, n_rows, 2, 2);
    } else if (loader == "with-offsets") {
      ret = engine_loader::load_body_and_tokenized_body_and_token_offsets(
          this, line_doc_path, n_rows, 1, 2, 3);
    } else {
      throw std::runtime_error("Loader " + loader + " is not supported");
    }

    LOG(WARNING) << "Number of terms in inverted index: " << TermCount();
    return ret;
  }

  void AddDocument(const std::string &body, const std::string &tokenized_body) {
    AddDocumentReturnId(body, tokenized_body);
  }

  void AddDocument(const std::string &body, const std::string &tokenized_body, const std::string &token_offsets) {
    AddDocumentReturnId(body, tokenized_body, token_offsets);
  }

  DocIdType AddDocumentReturnId(const std::string &body, const std::string &tokens) {
    int doc_id = NextDocId();

    doc_store_.Add(doc_id, body);
    inverted_index_.AddDocument(doc_id, body, tokens);
    doc_lengths_.AddLength(doc_id, utils::count_terms(body));
    return doc_id;
  }

  DocIdType AddDocumentReturnId(const std::string &body, const std::string &tokens, const std::string &token_offsets) {
    int doc_id = NextDocId();

    doc_store_.Add(doc_id, body);
    inverted_index_.AddDocument(doc_id, body, tokens, token_offsets);
    doc_lengths_.AddLength(doc_id, utils::count_terms(body)); // TODO modify to count on offsets?
    return doc_id;
  }
  
  std::string GetDocument(const DocIdType &doc_id) {
    return doc_store_.Get(doc_id);
  }

  int TermCount() const {
    return inverted_index_.Size();
  }

  std::map<std::string, int> PostinglistSizes(const TermList &terms) {
    return inverted_index_.PostinglistSizes(terms);
  }

  int GetDocLength(const DocIdType &doc_id) {
    return doc_lengths_.GetLength(doc_id);
  }

  IntersectionResult Query(const TermList &terms) {
    return inverted_index_.FindIntersection(terms);
  }

  DocScoreVec Score(const IntersectionResult &result) {
    return score_docs(result, doc_lengths_);
  }

  std::vector<DocIdType> FindTopK(const DocScoreVec &doc_scores, int k) {
    return utils::find_top_k(doc_scores, k);
  }

  SearchResult ProcessQueryTogether(const SearchQuery &query) {
    LOG(FATAL) << "not implemented" << std::endl;
  }

  std::string GenerateSnippet(const DocIdType &doc_id, 
      const std::vector<const RankingPostingWithOffsets *> &postings, 
      const int n_passages) {
    OffsetsEnums res = {};

    for (int i = 0; i < postings.size(); i++) {
      res.push_back(Offset_Iterator(*postings[i]->GetOffsetPairs()));
    }

    return highlighter_.highlightOffsetsEnums(res, n_passages, doc_store_.Get(doc_id));
  }

  SearchResult Search(const SearchQuery &query) {
    if (query.query_processing_core == QueryProcessingCore::TOGETHER) {
      return ProcessQueryTogether(query);
    } else if (query.query_processing_core == QueryProcessingCore::BY_PHASE) {
      return ProcessQueryStepByStep(query);
    }
  }

  SearchResult ProcessQueryStepByStep(const SearchQuery &query) {
    SearchResult result;

    auto intersection_result = Query(query.terms);
    auto scores = Score(intersection_result);
    std::vector<DocIdType> top_k = FindTopK(scores, query.n_results);

    for (auto doc_id : top_k) {
      SearchResultEntry entry;
      entry.doc_id = doc_id;

      if (query.return_snippets == true) {
        auto row = intersection_result.GetRow(doc_id);
        entry.snippet = GenerateSnippet(doc_id, row, query.n_snippet_passages);
      }

      result.entries.push_back(entry);
    }

    return result;
  }

  std::string GenerateSnippet(const DocIdType &doc_id, 
                              const IntersectionResult::row_dict_t *row,
                              const int n_passages) {
    OffsetsEnums res = {};
    
    for (auto col_it = row->cbegin() ; col_it != row->cend(); col_it++) {
      auto p_posting = IntersectionResult::GetPosting(col_it);
      res.push_back(Offset_Iterator(*p_posting->GetOffsetPairs()));
    }

    return highlighter_.highlightOffsetsEnums(res,  n_passages, doc_store_.Get(doc_id));
  }
};



std::unique_ptr<SearchEngineServiceNew> CreateSearchEngine(
    std::string engine_type);

#endif

