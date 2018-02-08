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
  Positions positions_;
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

  StandardPosting(const int &doc_id, 
                 const int &term_frequency, 
                 const OffsetPairs &offset_pairs, 
                 const Positions &positions)
    :doc_id_(doc_id), term_frequency_(term_frequency), 
     offset_pairs_(offset_pairs), positions_(positions) {}

  const int GetDocId() const {return doc_id_;}
  const int GetTermFreq() const {return term_frequency_;}
  const OffsetPairs *GetOffsetPairs() const {return &offset_pairs_;}
  const Positions *GetPositions() const {return &positions_;}

  std::string ToString() {
    std::ostringstream oss;
    oss << "DocId: " << doc_id_ << " TF: " << term_frequency_ << " ";
    for (auto & pair : offset_pairs_) {
      oss << "(" << std::get<0>(pair) << "," << std::get<1>(pair) << "),";
    }

    return oss.str();
  }

  // Assume an imaginary offset=0 before the first real offset
  // 0 (delta to prev, delta to prev), (delta to prev, delta to prev), ..
  VarintBuffer EncodeOffsets() const {
    VarintBuffer buf;
    uint32_t last_offset = 0;
    uint32_t delta;
    for (auto & pair : offset_pairs_) {
      delta = std::get<0>(pair) - last_offset;
      buf.Append(delta);
      last_offset = std::get<0>(pair);

      delta = std::get<1>(pair) - last_offset;
      buf.Append(delta);
      last_offset = std::get<1>(pair);
    }

    return buf;
  }

  // Assume an imaginary position = 0 before the first real position
  VarintBuffer EncodePositions() const {
    VarintBuffer buf;
    uint32_t last_pos = 0;
    uint32_t delta;

    for (auto &position : positions_) {
      delta = position - last_pos;
      buf.Append(delta);
      last_pos = position;
    }

    return buf;
  }

  // content_size | doc_id_delta | TF | off_size | off1 | off2 | off1 | off2 | ... | pos 1 | pos 2 | ...
  std::string Encode() const {
    VarintBuffer info_buf;
    info_buf.Append(doc_id_);
    info_buf.Append(term_frequency_);

    VarintBuffer offset_buf;
    for (auto & pair : offset_pairs_) {
      offset_buf.Append(std::get<0>(pair));
      offset_buf.Append(std::get<1>(pair));
    }
    int off_size = offset_buf.Size(); 
    offset_buf.Prepend(off_size);

    VarintBuffer pos_buf;
    for (auto &pos : positions_) {
      pos_buf.Append(pos);
    }

    int content_size = info_buf.Size() + offset_buf.Size() + pos_buf.Size();

    VarintBuffer merge_buf;
    merge_buf.Append(content_size);
    merge_buf.Append(info_buf.Data());
    merge_buf.Append(offset_buf.Data());
    merge_buf.Append(pos_buf.Data());

    return merge_buf.Data();
  }
};

#endif
