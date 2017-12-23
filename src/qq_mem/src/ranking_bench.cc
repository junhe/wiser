#include <iostream>
#include <cassert>
#include <unordered_map>

#include <glog/logging.h>

#include "intersect.h"
#include "ranking.h"
#include "utils.h"
#include "posting_list_vec.h"


typedef std::vector<int> TopDocs;
typedef std::tuple<int, int> Offset;  //startoffset, endoffset
typedef std::vector<Offset> Offsets;

class TermWithOffset {
	public:
		Term term_;
		Offsets offsets_;

		TermWithOffset(Term term_in, Offsets offsets_in) 
			: term_(term_in), offsets_(offsets_in) {} 
};

class RankingPosting {
  public:
    int doc_id_;
    int term_frequency_;
    int field_length_;

    RankingPosting(){};
    RankingPosting(int doc_id, int term_frequency, int field_length)
      :doc_id_(doc_id), term_frequency_(term_frequency), field_length_(field_length) 
    {
    }
    const int GetDocId() const {return doc_id_;}
    const int GetTermFreq() const {return term_frequency_;}
    const int GetFieldLength() const {return field_length_;}
};

typedef std::vector<TermWithOffset> TermWithOffsetList;


// Parse offsets from string
void handle_positions(const std::string& s, Offset& this_position) {
    std::size_t pos_split = s.find(",");
    this_position = std::make_tuple(std::stoi(s.substr(0, pos_split)), std::stoi(s.substr(pos_split+1)));
    // construct position
    return;
}


void handle_term_offsets(const std::string& s, Offsets& this_term) {
    std::string buff{""};

    for(auto n:s) {
        // split by ;
        if(n != ';') buff+=n; else
        if(n == ';' && buff != "") {
            Offset this_position;
            // split by ,
            handle_positions(buff, this_position);
            this_term.push_back(this_position);
            buff = "";
        }
    }

    return;
}

const std::vector<Offsets> parse_offsets(const std::string& s) {
    std::vector<Offsets> res;
    
    std::string buff{""};

    for(auto n:s)
    {
    // split by .
        if(n != '.') buff+=n; else
        if(n == '.' && buff != "") {
            Offsets this_term;
            handle_term_offsets(buff, this_term);
            res.push_back(this_term);
            buff = "";
        }
    }
    return res;
}

// return:
//   term1: offsets
//   term2: offsets
//   ...
TermWithOffsetList parse_doc(const std::vector<std::string> &items) {
	auto title = items[0];
	auto body = items[1];
	auto tokens = items[2];
	auto offsets = items[3];

	std::vector<Offsets> offsets_parsed = parse_offsets(offsets);
	std::vector<std::string> terms = utils::explode(tokens, ' ');
	TermWithOffsetList terms_with_offset = {};

	for (int i = 0; i < terms.size(); i++) {
		Offsets tmp_offsets = offsets_parsed[i];
		TermWithOffset cur_term(terms[i], tmp_offsets);
		terms_with_offset.push_back(cur_term);
	}

	return terms_with_offset;
}


class InvertedIndexQqMem {
 private:
  typedef PostingList_Vec<RankingPosting> PostingListType;
  typedef std::unordered_map<Term, PostingListType> IndexStore;
  IndexStore index_;

 public:
  typedef IndexStore::const_iterator const_iterator;

  void AddDocument(const int &doc_id, const TermList &termlist) {
     for (const auto &term : termlist) {
        IndexStore::iterator it;
        it = index_.find(term);

        if (it == index_.cend()) {
            // term does not exist
            std::pair<IndexStore::iterator, bool> ret;
            ret = index_.insert( std::make_pair(term, PostingListType(term)) );
            it = ret.first;
        } 

        PostingListType &postinglist = it->second;        
        postinglist.AddPosting(RankingPosting(doc_id, 100, 1000));
    }
  }

  std::vector<int> Search(const TermList &terms, const SearchOperator &op) {

  }

  void ShowStats() {
    LOG(INFO) << "num of terms: " << index_.size() << std::endl;
    for (auto it = index_.begin(); it != index_.end(); it++) {
      // LOG(INFO) << it->second.Size();
    }
  }
};

template <class T>
void print_vec(T vec) {
  for (auto e : vec) {
    std::cout << e << "|";
  }
  std::cout << std::endl;
}


int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 

  InvertedIndexQqMem inverted_index;

  utils::LineDoc linedoc("./src/testdata/line_doc_offset_untracked");

  for (int i = 0; i < 5; i++) {
    // Process a document
    std::cout << i << std::endl;
    std::vector<std::string> items;
    linedoc.GetRow(items);
    auto terms_with_offset = parse_doc(items);

    auto title = items[0];
    auto body = items[1];
    auto tokens = items[2];
    auto offsets = items[3];

    TermList terms = utils::explode(tokens, ' ');
    inverted_index.AddDocument(i, terms);
	}

  inverted_index.ShowStats();
}

