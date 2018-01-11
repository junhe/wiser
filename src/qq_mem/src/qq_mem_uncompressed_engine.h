#ifndef QQ_MEM_UNCOMPRESSED_ENGINE_H
#define QQ_MEM_UNCOMPRESSED_ENGINE_H


#include "doc_length_store.h"
#include "intersect.h"
#include "utils.h"
#include "native_doc_store.h"
#include "unifiedhighlighter.h"
#include "engine_loader.h"

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

  void AddDocument(const int &doc_id, const std::string &body, 
      const std::string &tokens) {
    TermList token_vec = utils::explode(tokens, ' ');
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
  int LoadLocalDocuments(const std::string &line_doc_path, int n_rows) {
    int ret = engine_loader::load_body_and_tokenized_body(
        this, line_doc_path, n_rows, 2, 2);

    LOG(WARNING) << "Number of terms in inverted index: " << TermCount();
    return ret;
  }

  void AddDocument(const std::string &body, const std::string &tokenized_body) {
    AddDocumentReturnId(body, tokenized_body);
  }

  DocIdType AddDocumentReturnId(const std::string &body, const std::string &tokens) {
    int doc_id = NextDocId();

    doc_store_.Add(doc_id, body);
    inverted_index_.AddDocument(doc_id, body, tokens);
    doc_lengths_.AddLength(doc_id, utils::count_terms(body));
    return doc_id;
  }

  std::string GetDocument(const DocIdType &doc_id) {
    return doc_store_.Get(doc_id);
  }

  std::size_t TermCount() {
    return inverted_index_.Size();
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

  SearchResult Search(const SearchQuery &query) {
    auto intersection_result = Query(query.terms);
    auto scores = Score(intersection_result);
    std::vector<DocIdType> top_k = FindTopK(scores, 10);

    SearchResult result;
    for (auto doc_id : top_k) {
      SearchResultEntry entry;
      entry.doc_id = doc_id;

      if (query.return_snippets == true) {
        auto row = intersection_result.GetRow(doc_id);
        entry.snippet = GenerateSnippet(doc_id, row);
      }

      result.entries.push_back(entry);
    }

    return result;
  }

  std::string GenerateSnippet(const DocIdType &doc_id, 
                              const IntersectionResult::row_dict_t *row) {
    OffsetsEnums res = {};
    
    for (auto col_it = row->cbegin() ; col_it != row->cend(); col_it++) {
      auto p_posting = IntersectionResult::GetPosting(col_it);
      res.push_back(Offset_Iterator(*p_posting->GetOffsetPairs()));
    }

    return highlighter_.highlightOffsetsEnums(res,  2, doc_store_.Get(doc_id));
  }
};


std::unique_ptr<SearchEngineServiceNew> CreateSearchEngine(
    std::string engine_type) {
  if (engine_type == "qq_mem_uncompressed") {
    return std::unique_ptr<SearchEngineServiceNew>(new QqMemUncompressedEngine);
  } else {
    throw std::runtime_error("Wrong engine type: " + engine_type);
  }
}


#endif

