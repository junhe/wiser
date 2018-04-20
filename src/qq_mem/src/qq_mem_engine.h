#ifndef QQ_MEM_ENGINE_H
#define QQ_MEM_ENGINE_H

#include <assert.h>

#include <boost/filesystem.hpp>

#include "posting.h"
#include "doc_length_store.h"
#include "query_processing.h"
#include "utils.h"
#include "doc_store.h"
#include "highlighter.h"
#include "engine_loader.h"
#include "posting_list_delta.h"
#include "general_config.h"


class InvertedIndexImpl: public InvertedIndexService {
 public:
  virtual void AddDocument(const int doc_id, const DocInfo doc_info) {
    auto format = doc_info.Format();

    if (format == "TOKEN_ONLY") {
      AddDocumentNaive(doc_id, doc_info.Body(), doc_info.Tokens());
    } else if (format == "WITH_OFFSETS") {
      AddDocumentWithOffsets(doc_id, doc_info.Body(), doc_info.Tokens(),
          doc_info.TokenOffsets());
    } else if (format == "WITH_POSITIONS") {
			AddDocumentWithPositions(doc_id, doc_info);
    } else {
      LOG(FATAL) << "Format " << format << " is not supported.\n";
    }
  }

 protected:
  virtual void AddDocumentNaive(const int &doc_id, const std::string &body, 
      const std::string &tokens) = 0;
  virtual void AddDocumentWithOffsets(const int &doc_id, const std::string &body, 
      const std::string &tokens, const std::string &token_offsets) = 0;
  virtual void AddDocumentWithPositions(const int &doc_id, 
      const DocInfo &doc_info) = 0;
};


class InvertedIndexQqMemDelta: public InvertedIndexImpl {
 protected:
  typedef PostingListDelta PostingListType;
  typedef std::unordered_map<Term, PostingListType> IndexStore;
  IndexStore index_;

 public:
  typedef IndexStore::const_iterator const_iterator;
  typedef std::vector<const PostingListType*> PlPointers;

  void Serialize(std::string path) const {
    std::ofstream ofile(path, std::ios::binary);

    int count = index_.size();
    ofile.write((char *)&count, sizeof(count));

    for (auto it = index_.cbegin(); it != index_.cend(); it++) {
      int term_len = it->first.size();
      ofile.write((char *)&term_len, sizeof(int));
      ofile.write(it->first.data(), term_len);

      std::string data = it->second.Serialize();
      int data_len = data.size();
      ofile.write((char *)&data_len, sizeof(int));
      ofile.write(data.data(), data_len);
    }

    ofile.close();	
  }

  int DeserializeEntry(const char *buf) {
    off_t offset = 0;

    int term_len = *((int *)buf); 
    offset += sizeof(int);

    std::string term(buf + offset, term_len);
    offset += term_len;

    int data_len = *((int *)(buf + offset)); 
    offset += sizeof(int);

    std::string data(buf + offset, data_len);
    offset += data_len;

    PostingListDelta pl("");
    pl.Deserialize(data, 0);

    index_.emplace(term, pl);

    // return the length of this entry
    return offset;
  }

  void Deserialize(std::string path) {
    int fd;
    int len;
    char *addr;
    size_t file_length;
    uint32_t var;
    off_t offset = 0;

    index_.clear();

    utils::MapFile(path, &addr, &fd, &file_length);

    int count = *((int *)addr);
    offset += sizeof(int);
    
    int width = sizeof(int);
    for (int i = 0; i < count; i++) {
      int entry_len = DeserializeEntry(addr + offset);
      offset += entry_len;
    }

    utils::UnmapFile(addr, fd, file_length);
  }

  IteratorPointers FindIterators(const TermList &terms) const {
    IteratorPointers it_pointers;
    PlPointers pl_pointers = FindPostinglists(terms);
    for (auto &pl : pl_pointers) {
      if (pl != nullptr) {
        it_pointers.push_back(std::move(pl->Begin()));
      }
    }

    return it_pointers;
  }

  std::vector<PostingListDeltaIterator> FindIteratorsSolid(const TermList &terms) const {
    std::vector<PostingListDeltaIterator> iterators;
    PlPointers pl_pointers = FindPostinglists(terms);
    for (auto &pl : pl_pointers) {
      if (pl != nullptr) {
        iterators.push_back(pl->Begin2());
      }
    }

    return iterators;
  }

  PlPointers FindPostinglists(const TermList &terms) const {
    PlPointers postinglist_pointers;

    for (auto term : terms) {
      auto it = index_.find(term);
      if (it == index_.end()) {
        postinglist_pointers.push_back(nullptr);
      } else {
        postinglist_pointers.push_back(&it->second);
      }
    }

    return postinglist_pointers;
  }

  std::map<std::string, int> PostinglistSizes(const TermList &terms) const {
    std::map<std::string, int> ret;

    auto pointers = FindPostinglists(terms);
    for (int i = 0; i < terms.size(); i++) {
      if (pointers[i] != nullptr) {
        ret[terms[i]] = pointers[i]->Size();
      } else {
        ret[terms[i]] = 0;
      }
    }

    return ret;
  }

  bool operator == (const InvertedIndexService &rhs) const {
    const InvertedIndexQqMemDelta &idx2 = static_cast<const InvertedIndexQqMemDelta &>(rhs);

    return this->index_ == idx2.index_;
  }

  bool operator != (const InvertedIndexService &rhs) const {
    return !(*this == rhs);
  }

  // Return number of posting lists
  int Size() const {
    return index_.size();
  }

 protected:
	void AddDocumentWithPositions(const int &doc_id, const DocInfo &doc_info) {
		TermList token_vec = doc_info.GetTokens();
		std::vector<Offsets> offsets_parsed = doc_info.GetOffsetPairsVec();
		std::vector<Positions> positions_table = doc_info.GetPositions();

		assert(token_vec.size() == offsets_parsed.size());

		for (int i = 0; i < token_vec.size(); i++) {
			IndexStore::iterator it = index_.find(token_vec[i]);

			if (it == index_.cend()) {
				std::pair<IndexStore::iterator, bool> ret;
				ret = index_.insert( std::make_pair(token_vec[i], PostingListType(token_vec[i])) );
				it = ret.first;
			} 

			PostingListType &postinglist = it->second;
			postinglist.AddPosting(
					StandardPosting(doc_id, offsets_parsed[i].size(), offsets_parsed[i],
						positions_table[i]));
		}
	}

  void AddDocumentNaive(const int &doc_id, const std::string &body, 
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
          StandardPosting(doc_id, token_it->second, term_offsets[term]));
    }
  }

  void AddDocumentWithOffsets(const int &doc_id, const std::string &body, 
      const std::string &tokens, const std::string &token_offsets) {
    TermList token_vec = utils::explode(tokens, ' ');
    std::vector<Offsets> offsets_parsed = utils::parse_offsets(token_offsets);
    
    assert(token_vec.size() == offsets_parsed.size());

    for (int i = 0; i < token_vec.size(); i++) {
      IndexStore::iterator it = index_.find(token_vec[i]);

      if (it == index_.cend()) {
        // term does not exist
        std::pair<IndexStore::iterator, bool> ret;
        ret = index_.insert( std::make_pair(token_vec[i], PostingListType(token_vec[i])) );
        it = ret.first;
      } 
      
      PostingListType &postinglist = it->second;
      postinglist.AddPosting(
          StandardPosting(doc_id, offsets_parsed[i].size(), offsets_parsed[i]));
    }
  }


};


class QqMemEngineDelta: public SearchEngineServiceNew {
 public:
  QqMemEngineDelta() {}

  // colum 2 should be tokens
  int LoadLocalDocuments(const std::string &line_doc_path, 
      int n_rows, const std::string format) {
    int ret;
    std::unique_ptr<LineDocParserService> parser;

    if (format == "TOKEN_ONLY") {
      parser.reset(new LineDocParserToken(line_doc_path, n_rows));
    } else if (format == "WITH_OFFSETS") {
      parser.reset(new LineDocParserOffset(line_doc_path, n_rows));
    } else if (format == "WITH_POSITIONS") {
      parser.reset(new LineDocParserPosition(line_doc_path, n_rows));
    } else {
      throw std::runtime_error("Format " + format + " is not supported");
    }

    DocInfo doc_info;
    while (parser->Pop(&doc_info)) {
      AddDocument(doc_info); 
      if (parser->Count() % 10000 == 0) {
        std::cout << "Indexed " << parser->Count() << " documents" << std::endl;
      }
    }

    LOG(WARNING) << "Number of terms in inverted index: " << TermCount();
    return ret;
  }

  void AddDocument(const DocInfo doc_info) {
    int doc_id = NextDocId();

    doc_store_.Add(doc_id, doc_info.Body());
    inverted_index_.AddDocument(doc_id, doc_info);
    doc_lengths_.AddLength(doc_id, doc_info.BodyLength()); // TODO modify to count on offsets?
    similarity_.Reset(doc_lengths_.GetAvgLength());
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

  std::string GenerateSnippet(const DocIdType &doc_id, 
      std::vector<OffsetPairs> &offset_table,
      const int n_passages) {
    OffsetsEnums res = {};

    for (int i = 0; i < offset_table.size(); i++) {
      res.push_back(Offset_Iterator(offset_table[i]));
    }

    return highlighter_.highlightOffsetsEnums(res, n_passages, doc_store_.Get(doc_id));
  }

  SearchResult Search(const SearchQuery &query) {
    SearchResult result;

    if (query.n_results == 0) {
      return result;
    }

    std::vector<PostingListDeltaIterator> iterators = 
        inverted_index_.FindIteratorsSolid(query.terms);

    if (iterators.size() == 0 || iterators.size() < query.terms.size()) {
      return result;
    }

    auto top_k = qq_search::ProcessQueryDelta<PostingListDeltaIterator, CompressedPositionIterator>(
        similarity_, &iterators, doc_lengths_, doc_lengths_.Size(), 
        query.n_results, query.is_phrase);  

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

  friend bool operator == (const QqMemEngineDelta &a, const QqMemEngineDelta &b) {
    if (a.next_doc_id_ != b.next_doc_id_) {
      return false;
    }

    if (a.doc_store_.Size() != b.doc_store_.Size()) {
      return false;
    }

    if (a.inverted_index_ != b.inverted_index_) {
      return false;
    }

    if (a.doc_lengths_.Size() != b.doc_lengths_.Size()) {
      return false;
    }

    return true;
  }

  void SerializeMeta(std::string path) const {
    std::ofstream ofile(path, std::ios::binary);

    ofile.write((char *)&next_doc_id_, sizeof(next_doc_id_));

    ofile.close();	
  }

  void DeserializeMeta(std::string path) {
    int fd;
    int len;
    char *addr;
    size_t file_length;
    uint32_t var;

    utils::MapFile(path, &addr, &fd, &file_length);

    next_doc_id_ = *((int *)addr);
    
    utils::UnmapFile(addr, fd, file_length);
  }

  void Serialize(std::string dir_path) const {
    namespace boost_fs = boost::filesystem;
    boost_fs::path p{dir_path};

    if (boost_fs::exists(p) == false) {
      boost_fs::create_directory(p);
    }

    SerializeMeta(dir_path + "/engine_meta.dump");
    doc_store_.Serialize(dir_path + "/doc_store.dump");
    inverted_index_.Serialize(dir_path + "/inverted_index.dump");
    doc_lengths_.Serialize(dir_path + "/doc_lengths.dump");
  }

  void Deserialize(std::string dir_path) {
    DeserializeMeta(dir_path + "/engine_meta.dump"); //good
    doc_store_.Deserialize(dir_path + "/doc_store.dump"); //good
    inverted_index_.Deserialize(dir_path + "/inverted_index.dump");
    doc_lengths_.Deserialize(dir_path + "/doc_lengths.dump");
    similarity_.Reset(doc_lengths_.GetAvgLength());

    std::cout << "Doc lengths _______________________________________________________\n";
    doc_lengths_.Show();
  }

 private:
  int next_doc_id_ = 0;
  CompressedDocStore doc_store_;
  InvertedIndexQqMemDelta inverted_index_;
  DocLengthCharStore doc_lengths_;
  SimpleHighlighter highlighter_;
  Bm25Similarity similarity_;

  int NextDocId() {
    return next_doc_id_++;
  }
};


#endif

