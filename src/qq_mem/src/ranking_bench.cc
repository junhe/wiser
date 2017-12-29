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

    const int CalcTermFreq() const {
      return offsets_.size();  
    }
};
typedef std::vector<TermWithOffset> TermWithOffsetList;

class FieldLengthStore {
 public:
  std::unordered_map<DocIdType, int> store_;

  void SetLength(DocIdType doc_id, int len) {
    store_[doc_id] = len;
  }

  int GetLength(DocIdType doc_id) {
    return store_[doc_id];
  }
};



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


int calc_total_terms(const TermWithOffsetList &termlist) {
  int total = 0;
  for (const auto &term_with_offset : termlist) {
    total += term_with_offset.CalcTermFreq(); 
  }
  return total;
}

typedef std::unordered_map<Term, int> CountMapType;
CountMapType count_tokens(const std::string &token_text) {
  CountMapType counts;
  auto tokens = utils::explode(token_text, ' ');

  for (auto token : tokens) {
    auto it = counts.find(token);
    if (it == counts.end()) {
      counts[token] = 1;
    } else {
      it->second++;
    }
  }
  return counts;
}

// It returns the number of terms in field
int calc_terms(const std::string field) {
  return utils::explode(field, ' ').size();
}

void print_counts(CountMapType counts) {
  for (auto it = counts.begin(); it != counts.end(); it++) {
    std::cout << it->first << ":" << it->second << std::endl;
  }
}

class InvertedIndexQqMem {
 private:
  typedef PostingList_Vec<RankingPosting> PostingListType;
  typedef std::unordered_map<Term, PostingListType> IndexStore;
  IndexStore index_;

 public:
  typedef IndexStore::const_iterator const_iterator;

  void AddDocument(const int &doc_id, const std::string &body, 
      const std::string &tokens) {
    TermList token_vec = utils::explode(tokens, ' ');
    CountMapType token_counts = count_tokens(tokens);

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
      postinglist.AddPosting(RankingPosting(doc_id, token_it->second));
    }
    int total_terms = calc_terms(tokens);
    std::cout << "field length(total terms): " << total_terms << std::endl;
  }

  std::vector<DocIdType> Search(const TermList &terms, const SearchOperator &op) {
    if (op != SearchOperator::AND) {
        throw std::runtime_error("NotImplementedError");
    }

    std::vector<const PostingList_Vec<RankingPosting>*> postinglist_pointers;

    for (auto term : terms) {
      auto it = index_.find(term);
      if (it == index_.end()) {
        std::cout << "No match at all" << std::endl;
        return std::vector<DocIdType>{};
      }

      postinglist_pointers.push_back(&it->second);
    }

    TfIdfStore tfidf_store;

    return intersect<RankingPosting>(postinglist_pointers, &tfidf_store);
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

void test() {
  {
    CountMapType counts = count_tokens("hello hello you");
    assert(counts["hello"] == 2);
    assert(counts["you"] == 1);
    assert(counts.size() == 2);
  }

  {
    assert(calc_terms("hello world") == 2);
  }

  {
    FieldLengthStore store;
    store.SetLength(3, 20);
    store.SetLength(4, 21);
    assert(store.GetLength(3) == 20);
    assert(store.GetLength(4) == 21);
  }

  std::cout << "_____________________" << std::endl;
  {
    InvertedIndexQqMem inverted_index;
    utils::LineDoc linedoc("./src/testdata/tiny-line-doc");

    for (int i = 0; i < 2; i++) {
      std::vector<std::string> items;
      linedoc.GetRow(items);
      inverted_index.AddDocument(i, items[0], items[1]);
    }
    // inverted_index.Search(TermList{"you"}, SearchOperator::AND);
    // auto result = inverted_index.SearchAndRank(TermList{"have"}, SearchOperator::AND);
    TfIdfStore tfidf_store = inverted_index.RetrieveTfIdf(TermList{"have"});
    print_vec(result);
  }
  std::cout << "_____________________" << std::endl;
}


int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  // FLAGS_logtostderr = 1; // print to stderr instead of file
  FLAGS_stderrthreshold = 0; 
  FLAGS_minloglevel = 0; 

  test();

  InvertedIndexQqMem inverted_index;
  // This could be put to doc store in the future
  FieldLengthStore field_lengths; 

  utils::LineDoc linedoc("./src/testdata/tiny-line-doc");

  for (int i = 0; i < 2; i++) {
    // Process a document
    std::cout << i << std::endl;
    std::vector<std::string> items;
    linedoc.GetRow(items);

    std::cout << "body: " << items[0] << std::endl;
    std::cout << "tokens: " << items[1] << std::endl;

    inverted_index.AddDocument(i, items[0], items[1]);
    field_lengths.SetLength(i, calc_terms(items[1]));
	}

  inverted_index.ShowStats();
}

