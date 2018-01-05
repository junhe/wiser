#include "posting.h"
#include "posting_list_protobuf.h"

#include <string.h>
#include <iostream>

//Posting
Posting::Posting(const int & docID, const int & term_frequency, const Offsets & offsets_in)
    :docID_(docID), positions_(offsets_in), term_frequency_(term_frequency)
{ // TODO next?
  // get precomputed passage scores
    if (FLAG_SNIPPETS_PRECOMPUTE) {
        /*
        std::cout<< "Before: "; 
        for (auto offset: offsets_in ) {
            std::cout << std::get<0>(offset) << "," << std::get<1>(offset) << ";";
        }
        std::cout << std::endl;
        */
        // find (-1, -1)  = cur_pos - 1
        int len_offsets = offsets_in.size();
        int cur_pos = std::max(0, len_offsets - 2*(std::get<0>(offsets_in[len_offsets-1])+1) - 1);
        while (std::get<0>(offsets_in[cur_pos]) != -1) {
            cur_pos++;
        }
        cur_pos++;

        // get positions
        positions_.assign(offsets_in.begin(), offsets_in.begin() + cur_pos-1);
        term_frequency_ = cur_pos-1;
        
        // get precomputed scores & splits
        for (; cur_pos < len_offsets; cur_pos+=2) {
            float score = (float)(std::get<1>(offsets_in[cur_pos])) / 100000000;
            int passage_id = std::get<0>(offsets_in[cur_pos]);
            passage_scores_.push_back(Passage_Score(passage_id, score));
            passage_splits_[passage_id] = Passage_Split(std::get<0>(offsets_in[cur_pos+1]), std::get<1>(offsets_in[cur_pos+1]));
        }
        /*
        std::cout<< "After : "; 
        for (auto offset: positions_ ) {
            std::cout << std::get<0>(offset) << "," << std::get<1>(offset) << ";";
        }
        std::cout << std::endl;
        for (auto score : passage_scores_) {
            std::cout << score.first << "," << score.second << ";";
        }
        std::cout << std::endl;
        for (auto split : passage_splits_) {
            std::cout << split.second.first << "," << split.second.second << ";";
        }
        std::cout << std::endl;
        */
    }
}


Posting::Posting()
    :docID_(0), term_frequency_(0)
{}

Posting::Posting(const int & docID, const int & term_frequency, 
                 Offsets * offsets_in, Passage_Scores * passage_scores, 
                 std::unordered_map<int, Passage_Split> * passage_splits)
    :docID_(docID), term_frequency_(term_frequency)
{   // you need to set those varaiables by yourself, pass-by pointer prevents one copy
    offsets_in = &positions_;
    passage_scores = &passage_scores_;
    passage_splits = &passage_splits_;
}


std::string Posting::dump() {    // serialize function
    // use protobuf
    /*
    message Posting_Precomputed_4_Snippets {
  int32 docID = 1;
  int32 term_frequency = 2;
  repeated Offset positions = 3;  // vector of positions
  repeated Passage_Score passage_scores = 4;
   }

    */
    if (!FLAG_SNIPPETS_PRECOMPUTE) 
        return "";
    

    std::string p_string;
    if (POSTING_SERIALIZATION == "protobuf") {
        posting_message::Posting_Precomputed_4_Snippets p_message;
        {
            // docID
            p_message.set_docid(docID_);
            // term_frequency
            p_message.set_term_frequency(term_frequency_);
            // Offsets
            for (int i = 0; i < term_frequency_; i++) {
                posting_message::Offset * new_offset = p_message.add_offsets();
                new_offset->set_start_offset(std::get<0>(positions_[i]));
                new_offset->set_end_offset(std::get<1>(positions_[i]));
            }

            //passage_scores
            for (int i = 0; i < passage_scores_.size(); i++) {
                posting_message::Passage_Score * new_passage_score = p_message.add_passage_scores();
                new_passage_score->set_passage_id(passage_scores_[i].first);
                new_passage_score->set_score(passage_scores_[i].second);
            }
            // passage_splits  could be faster?
            auto & splits = *p_message.mutable_passage_splits();
            for (auto cur_split:passage_splits_) {
                posting_message::Passage_Split new_split;
                new_split.set_start_offset(cur_split.second.first);
                new_split.set_len(cur_split.second.second);
                splits[cur_split.first] = new_split;
            }
        }
        p_message.SerializeToString(&p_string);
    }
    
    if (POSTING_SERIALIZATION == "cereal") {
        std::stringstream ss; // any stream can be used

        {
          cereal::BinaryOutputArchive oarchive(ss); // Create an output archive

          oarchive(*this); // Write the data to the archive
        } // archive goes out of scope, ensuring all contents are flushed
   /*{
    cereal::BinaryInputArchive iarchive(ss); // Create an input archive

    Posting tmp_posting;
    iarchive(tmp_posting); // Read the data from the archive
    std::cout << docID_ << ":" << term_frequency_ << ":" 
              << positions_.size() << ":"
              << passage_scores_.size() << ": " << passage_splits_.size()
              << std::endl;
    std::cout << tmp_posting.docID_ << ":" << tmp_posting.term_frequency_ << ":" 
              << tmp_posting.positions_.size() << ":"
              << tmp_posting.passage_scores_.size() << ": " << tmp_posting.passage_splits_.size()
              << std::endl;

   } */

        return ss.str();
    }


    return p_string;
}

