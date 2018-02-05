#ifndef POSTING_H
#define POSTING_H

#include <unordered_map>
#include <cereal/types/vector.hpp>
#include <cereal/types/tuple.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/unordered_map.hpp>
#include <cereal/archives/binary.hpp>
#include <sstream>

#include "posting_message.pb.h"
#include "engine_services.h"
#include "types.h"
#include "utils.h"
#include "compression.h"


typedef int Position;
typedef std::vector<Position> Positions;


class PostingSimple : public QqMemPostingService {
 public:
  int docID_;
  int term_frequency_;
  Positions positions_;

  PostingSimple() :docID_(0), term_frequency_(0) {}
  PostingSimple(int docID, int term_frequency, const Positions positions)
        :docID_(docID), term_frequency_(term_frequency), positions_(positions)
  {}

  const int GetDocId() const {return docID_;}
  const int GetTermFreq() const {return term_frequency_;}
  const Positions GetPositions() const {return positions_;}
  const OffsetPairs *GetOffsetPairs() const {
    throw std::runtime_error("Not implemented"); 
  }
  std::string dump() {return "";}
};


class StandardPosting: public QqMemPostingService {
 protected:
  OffsetPairs offset_pairs_;
  int doc_id_;
  int term_frequency_;

 public:
  StandardPosting(){}
  StandardPosting(const int &doc_id, 
                 const int &term_frequency)
    :doc_id_(doc_id), term_frequency_(term_frequency){}

  StandardPosting(const int &doc_id, 
                 const int &term_frequency, 
                 const OffsetPairs &offset_pairs)
    :doc_id_(doc_id), term_frequency_(term_frequency), 
     offset_pairs_(offset_pairs) {}

  const int GetDocId() const {return doc_id_;}
  const int GetTermFreq() const {return term_frequency_;}
  const OffsetPairs *GetOffsetPairs() const {return &offset_pairs_;}

  std::string ToString() {
    std::ostringstream oss;
    oss << "DocId: " << doc_id_ << " TF: " << term_frequency_ << " ";
    for (auto & pair : offset_pairs_) {
      oss << "(" << std::get<0>(pair) << "," << std::get<1>(pair) << "),";
    }

    return oss.str();
  }

  // content_size | doc_id_delta | TF | off1 | off2 | off1 | off2 | ...
  std::string Encode() const {
    VarintBuffer buf;

    buf.Append(doc_id_);
    buf.Append(term_frequency_);

    for (auto & pair : offset_pairs_) {
      buf.Append(std::get<0>(pair));
      buf.Append(std::get<1>(pair));
    }

    buf.Prepend(buf.Size());

    return buf.Data();
  }
};

#endif
