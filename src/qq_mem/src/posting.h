#ifndef POSTING_H
#define POSTING_H

#include <unordered_map>
#include <cereal/types/vector.hpp>
#include <cereal/types/tuple.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/unordered_map.hpp>
#include <cereal/archives/binary.hpp>
#include <sstream>

#include "engine_services.h"
#include "types.h"
#include "utils.h"
#include "compression.h"


// TODO will delete them
typedef int Position;
typedef std::vector<Position> Positions;


class Posting : public PostingService {
    public:
        int docID_ = 0;
        int term_frequency_ = 0;
        Offsets positions_ = {};
        
        Passage_Scores passage_scores_ = {};
        std::unordered_map<int, Passage_Split> passage_splits_; // passage_id to passage_split
 
        Posting();
        Posting(const int & docID, const int & term_frequency, const Offsets & offsets_in);
        Posting(const int & docID, const int & term_frequency, 
                Offsets * offsets_in, Passage_Scores * passage_scores, std::unordered_map<int, Passage_Split> * passage_splits);
        std::string dump();

        // This method lets cereal know which data members to serialize
        template<class Archive>
        void serialize(Archive & archive)
        {
            archive( docID_, term_frequency_, positions_, passage_scores_, passage_splits_); // serialize things by passing them to the archive
        }
        // TODO Posting * next;
};


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


class RankingPosting : public QqMemPostingService {
 protected:
  int doc_id_;
  int term_frequency_;

 public:
  RankingPosting(){};
  RankingPosting(int doc_id, int term_frequency)
    :doc_id_(doc_id), term_frequency_(term_frequency)
  {}
  const int GetDocId() const {return doc_id_;}
  const int GetTermFreq() const {return term_frequency_;}
  const OffsetPairs *GetOffsetPairs() const {
    throw std::runtime_error("Not implemented"); 
  }
};

// This is class is created because I do not want to modify 
// RankingPosting. The modifications would incur changes in many other places.
class StandardPosting: public RankingPosting {
 protected:
  OffsetPairs offset_pairs_;

 public:
  StandardPosting(const int &doc_id, 
                 const int &term_frequency)
    :RankingPosting(doc_id, term_frequency) {}

  StandardPosting(const int &doc_id, 
                 const int &term_frequency, 
                 const OffsetPairs &offset_pairs)
    :RankingPosting(doc_id, term_frequency), offset_pairs_(offset_pairs) {}

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
