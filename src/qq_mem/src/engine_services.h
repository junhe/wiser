#ifndef ENGINE_SERVICES_H
#define ENGINE_SERVICES_H


#include <string>
#include <vector>
#include <map>
#include <tuple>

#include <glog/logging.h>

#include "types.h"

class SearchEngineServiceNew {
 public:
  virtual void AddDocument(const DocInfo doc_info) = 0;
  virtual int LoadLocalDocuments(const std::string &line_doc_path, 
      int n_rows, const std::string loader) = 0;
  virtual int TermCount() const = 0;
  virtual std::map<std::string, int> PostinglistSizes(const TermList &terms) = 0;
  virtual SearchResult Search(const SearchQuery &query) = 0; 
  virtual void Serialize(std::string dir_path) const = 0;
  virtual void Deserialize(std::string dir_path) = 0;
  virtual void PrintByteCounts() const = 0;
};


struct DocScore {
  DocIdType doc_id;
  qq_float score;

  DocScore(const DocIdType &doc_id_in, const qq_float &score_in)
    :doc_id(doc_id_in), score(score_in) {}

  friend bool operator<(DocScore a, DocScore b)
  {
    return a.score < b.score;
  }

  std::string ToStr() {
    return std::to_string(doc_id) + " (" + std::to_string(score) + ")";
  }
};

typedef std::vector<DocScore> DocScoreVec;
typedef std::map<Term, qq_float> TermScoreMap;


class QqMemPostingService {
 public:
  virtual const int GetDocId() const = 0;
  virtual const int GetTermFreq() const = 0;
  virtual const OffsetPairs *GetOffsetPairs() const = 0;
};

class FlashReader {
    public:
        FlashReader(const std::string & based_file);
        ~FlashReader();
        std::string read(const Store_Segment & segment);
        Store_Segment append(const std::string & value);

    private:
        std::string _file_name_;
        int _fd_;
        int _last_offset_; // append from it
};


class OffsetPairsIteratorService {
 public:
  virtual bool IsEnd() const = 0;
  virtual void Pop(OffsetPair *pair) = 0;
};


class PopIteratorService {
 public:
  virtual bool IsEnd() const = 0;
  virtual uint32_t Pop() = 0;
};


class CompressedPositionIterator;

class PostingListIteratorService {
 public:
  virtual int Size() const = 0;

  virtual bool IsEnd() const = 0;
  virtual DocIdType DocId() const = 0;
  virtual int TermFreq() const = 0;

  virtual void Advance() = 0;
  virtual void SkipForward(const DocIdType doc_id) = 0;
  virtual std::unique_ptr<OffsetPairsIteratorService> OffsetPairsBegin() const = 0;
  virtual std::unique_ptr<PopIteratorService> PositionBegin() const = 0;
  virtual void AssignPositionBegin(CompressedPositionIterator *iterator) const {
    LOG(FATAL) << "Not Implemented"; };
};


typedef std::vector<std::unique_ptr<PostingListIteratorService>> IteratorPointers;
class InvertedIndexService {
 public:
  virtual IteratorPointers FindIterators(const TermList &terms) const = 0;
  virtual std::map<std::string, int> PostinglistSizes(const TermList &terms) const = 0;
  virtual void AddDocument(const int doc_id, const DocInfo doc_info) = 0;
  virtual int Size() const = 0;
  virtual void Serialize(std::string dir_path) const {
    LOG(FATAL) << "Not Implemented" << std::endl;
  }
  virtual void Deserialize(std::string dir_path) {
    LOG(FATAL) << "Not Implemented" << std::endl;
  }
  virtual bool operator== (const InvertedIndexService &rhs) const {
    LOG(FATAL) << "Not Implemented" << std::endl;
  }
  virtual bool operator!= (const InvertedIndexService &rhs) const {
    LOG(FATAL) << "Not Implemented" << std::endl;
  }
};


#endif
