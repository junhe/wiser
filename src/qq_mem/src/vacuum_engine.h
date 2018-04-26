#ifndef VACUUM_ENGINE_H
#define VACUUM_ENGINE_H

#include <iostream>
#include <string>

#include <gperftools/profiler.h>

#include "utils.h"
#include "flash_iterators.h"


class TermIndex {
 public:
  typedef std::unordered_map<Term, off_t> LocalMap;
  typedef LocalMap::const_iterator ConstIterator;

  void Load(const std::string &path) {
    int fd;
    char *addr;
    size_t file_length;
    utils::MapFile(path, &addr, &fd, &file_length);

    const char *end = addr + file_length;
    const char *buf = addr;

    int cnt = 0;
    while (buf < end) {
      buf = LoadEntry(buf);
      cnt++;
      if (cnt % 1000000 == 0) {
        std::cout << "Term index entries loaded: " << cnt << std::endl;
      }
    }

    utils::UnmapFile(addr, fd, file_length);
  }

  const char *LoadEntry(const char *buf) {
    uint32_t term_size = *((uint32_t *)buf);

    buf += sizeof(uint32_t);
    std::string term(buf , term_size);

    buf += term_size;
    off_t offset = *((off_t *)buf);

    index_[term] = offset;

    return buf + sizeof(off_t);
  }

  int NumTerms () const {
    return index_.size();
  }

  ConstIterator Find(const Term &term) {
    ConstIterator it = index_.find(term);
    return it;
  }

  ConstIterator CEnd() {
    return index_.cend();
  }

 private:
  LocalMap index_;
};


// To use it, you must
//
// 1. LoadTermIndex()
// 2. MapPostingLists()
//
// OR
// 1. Load()
//
// Or
// 1. Construct(path, path)
class VacuumInvertedIndex {
 public:
  VacuumInvertedIndex() {}

  VacuumInvertedIndex(
      const std::string term_index_path, 
      const std::string inverted_index_path)
  {
    Load(term_index_path, inverted_index_path);
  }

  void Load(
      const std::string term_index_path, 
      const std::string inverted_index_path) {

    LoadTermIndex(term_index_path);
    MapPostingLists(inverted_index_path);
  }

  void LoadTermIndex(const std::string term_index_path) {
    std::cout << "VacuumInvertedIndex Loading.............." << std::endl;
    std::cout << "Open term_index_path: " << term_index_path << std::endl;
    std::cout << "Loading term index ..................." << std::endl;
    term_index_.Load(term_index_path);
    std::cout << "Term index loaded ..................." << std::endl;
  }

  void MapPostingLists(const std::string inverted_index_path) {
    std::cout << "Open inverted_index_path: " << inverted_index_path << std::endl;
    file_map_.Open(inverted_index_path);
    file_data_ = (uint8_t *)file_map_.Addr();

    // utils::AdviseDoNotNeed(file_map_.Addr(), file_map_.Length());
  }

  off_t FindPostingListOffset(const Term term) {
    TermIndex::ConstIterator it = term_index_.Find(term);
    if (it == term_index_.CEnd()) {
      return -1;
    } else {
      return it->second;
    }
  }

  std::vector<VacuumPostingListIterator> FindIteratorsSolid(const TermList &terms) {
    std::vector<VacuumPostingListIterator> iterators;

    for (auto &term : terms) {
      off_t offset = FindPostingListOffset(term); 
      if (offset != -1) {
        iterators.emplace_back(file_data_, offset);
      }
    }
    return iterators;
  }

  int NumTerms() const {
    return term_index_.NumTerms();
  }


 private:
  TermIndex term_index_;  
  utils::FileMap file_map_;
  const uint8_t *file_data_; // = file_map_.Addr(), put it here for convenience
};


DECLARE_bool(profile_vacuum);

// To use
// engine = VacuumEngine(path)
// engine.Load()
class VacuumEngine : public SearchEngineServiceNew {
 public:
  VacuumEngine(const std::string engine_dir_path)
    :engine_dir_path_(engine_dir_path)
  {}

	~VacuumEngine() {
    std::cout << "--------------------------------------" << std::endl;
    std::cout << "Destructing VacuumEngine!!!!" << std::endl;
    std::cout << "--------------------------------------" << std::endl;

    if (profiler_started == true) {
      std::cout << "--------------------------------------" << std::endl;
			std::cout << "Stopping google profiler..." << std::endl;
      std::cout << "--------------------------------------" << std::endl;
      ProfilerStop();
    }
  }

  void Load() override {
    LOG_IF(FATAL, is_loaded_ == true) << "Engine is already loaded.";

    doc_store_.LoadFdx(utils::JoinPath(engine_dir_path_, "my.fdx"),
                    utils::JoinPath(engine_dir_path_, "my.fdt"));

    doc_lengths_.Deserialize(utils::JoinPath(engine_dir_path_, "my.doc_length"));

    similarity_.Reset(doc_lengths_.GetAvgLength());

    inverted_index_.LoadTermIndex(utils::JoinPath(engine_dir_path_, "my.tip"));

    // utils::LockAllMemory(); // <<<<<<< Lock memory

    inverted_index_.MapPostingLists(
        utils::JoinPath(engine_dir_path_, "my.vacuum"));

    doc_store_.MapFdt(utils::JoinPath(engine_dir_path_, "my.fdt"));

    is_loaded_ = true;

    if (FLAGS_profile_vacuum == true && profiler_started == false) {
      std::cout << "--------------------------------------" << std::endl;
			std::cout << "Using profiler..." << std::endl;
      std::cout << "--------------------------------------" << std::endl;
			ProfilerStart("vacuum.profile");
      profiler_started = true;
    }
  }

  int TermCount() const override {
    DLOG_IF(FATAL, is_loaded_ == false) << "Engine is not yet loaded";
    return inverted_index_.NumTerms(); 
  }

  std::map<std::string, int> PostinglistSizes(const TermList &terms) override {
    DLOG_IF(FATAL, is_loaded_ == false) << "Engine is not yet loaded";
    std::map<std::string, int> ret;

    for (auto &term : terms) {
      std::vector<VacuumPostingListIterator> iters = 
        inverted_index_.FindIteratorsSolid({term});
      if (iters.size() == 1) {
        ret[term] = iters[0].Size();
      }
    }
    return ret;
  }

  SearchResult Search(const SearchQuery &query) override {
    DLOG_IF(FATAL, is_loaded_ == false) << "Engine is not yet loaded";

    SearchResult result;

    if (query.n_results == 0) {
      return result;
    }

    std::vector<VacuumPostingListIterator> iterators = 
        inverted_index_.FindIteratorsSolid(query.terms);

    if (iterators.size() == 0 || iterators.size() < query.terms.size()) {
      return result;
    }

    // utils::PrintIter<VacuumPostingListIterator, InBagPositionIterator>(
        // iterators[0]);

    auto top_k = qq_search::ProcessQueryDelta
      <VacuumPostingListIterator, InBagPositionIterator>(
        similarity_,      &iterators,     doc_lengths_, doc_lengths_.Size(), 
        query.n_results,  query.is_phrase);  

    for (auto & top_doc_entry : top_k) {
      SearchResultEntry result_entry;
      result_entry.doc_id = top_doc_entry.doc_id;
      result_entry.doc_score = top_doc_entry.score;

      if (query.return_snippets == true) {
        auto offset_table = top_doc_entry.OffsetsForHighliting();
        result_entry.snippet = GenerateSnippet(top_doc_entry.doc_id,
            offset_table, query.n_snippet_passages);
      }

      result.entries.push_back(result_entry);
    }

    return result;
  }

  void Serialize(std::string dir_path) const override {
    LOG(FATAL) << "Not implemented in VacuumEngine.";
  }

  void Deserialize(std::string dir_path) override {
    LOG(FATAL) << "Not implemented in VacuumEngine.";
  }

  void AddDocument(const DocInfo doc_info) override {
    LOG(FATAL) << "Not implemented in VacuumEngine.";
  }

  int LoadLocalDocuments(const std::string &line_doc_path, 
     int n_rows, const std::string loader) override {
    LOG(FATAL) << "Not implemented in VacuumEngine.";
    return -1;
  }

 private:
  std::string GenerateSnippet(const DocIdType &doc_id, 
      std::vector<OffsetPairs> &offset_table,
      const int n_passages) {
    OffsetsEnums res = {};

    for (int i = 0; i < offset_table.size(); i++) {
      res.push_back(Offset_Iterator(offset_table[i]));
    }

    return highlighter_.highlightOffsetsEnums(res, n_passages, doc_store_.Get(doc_id));
  }


  VacuumInvertedIndex inverted_index_;
  DocLengthCharStore doc_lengths_;
  FlashDocStore doc_store_;

  SimpleHighlighter highlighter_;
  Bm25Similarity similarity_;

  std::string engine_dir_path_;
  bool is_loaded_ = false;
  bool profiler_started = false;
};




#endif

