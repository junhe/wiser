#ifndef UNIFIEDHIGHLIGHTER_H
#define UNIFIEDHIGHLIGHTER_H
#include "engine_services.h"
#include "types.h"
#include "qq_engine.h"
#include "lrucache.h"
#include <boost/locale.hpp>
#include <math.h> 
#include <fstream>

class PassageScore_Iterator {
 public:
   PassageScore_Iterator(const Passage_Scores & passage_scores_in) {
     _passage_scores_ = &passage_scores_in;
     _cur_passage_ = passage_scores_in.begin(); 
     cur_passage_id_ = (*_cur_passage_).first;
     score_ = (*_cur_passage_).second; 
   }

   void next_passage() {  // go to next passage
     _cur_passage_++;
     if (_cur_passage_ == _passage_scores_->end()) {
       cur_passage_id_ = -1;
       return ;
     } 
     cur_passage_id_ = (*_cur_passage_).first;
     score_ = (*_cur_passage_).second;
     return;
   }
  
  int cur_passage_id_;       // -1 means end
  int weight = 1;            // weight of this term
  float score_ = 0;          // score of this passage for this term
 private:
  const Passage_Scores * _passage_scores_;
  Passage_Scores::const_iterator _cur_passage_;
};


class Offset_Iterator {
 public:
  int startoffset;           // -1 means end
  int endoffset;
  int weight = 1;            // weight of this term

  Offset_Iterator(const std::vector<Offset> & offsets_in) {
      offsets = &offsets_in;
      cur_position = offsets_in.begin(); 
      startoffset = std::get<0>(*cur_position);
      endoffset = std::get<1>(*cur_position);
  }
  void next_position() {  // go to next offset position
    cur_position++;
    if (cur_position == offsets->end()) {
      startoffset = endoffset = -1;
      return ;
    } 
    startoffset = std::get<0>(*cur_position);
    endoffset = std::get<1>(*cur_position);
    return;
  }

 private:
  const Offsets * offsets;
  Offsets::const_iterator cur_position;
};


typedef std::vector<PassageScore_Iterator> ScoresEnums;
typedef std::vector<Offset_Iterator> OffsetsEnums;


class Passage {
  public:
    int startoffset = -1;
    int endoffset = -1;
    float score = 0;

    void reset() {
      startoffset = endoffset = -1;
      score = 0;
      matches = {};
    }

    void addMatch(const int & startoffset, const int & endoffset) {
      matches.push_back(std::make_tuple(startoffset, endoffset)); 
      return ;
    } 

    Offsets matches = {};
    std::string to_string(const std::string * doc_string) {
      std::string res= "";
      res += doc_string->substr(startoffset, endoffset - startoffset + 1) + "\n";
      // highlight

      auto cmp_terms = [] (Offset & a, Offset & b) -> bool { return (std::get<0>(a) > std::get<0>(b));};
      std::sort(matches.begin(), matches.end(), cmp_terms);

      for (auto it = matches.begin(); it != matches.end(); it++) {
        res.insert(std::get<1>(*it)-startoffset+1, "<\\b>");
        res.insert(std::get<0>(*it)-startoffset, "<b>");
      }
      //std::cout << std::endl << res;
      return res;
    }
};


class SentenceBreakIterator {
 public:
  SentenceBreakIterator(const std::string & content) {
    startoffset = endoffset = -1;
    content_ = & content;
    // start boost boundary analysis
    boost::locale::boundary::sboundary_point_index tmp(boost::locale::boundary::sentence, content_->begin(), content_->end(),gen("en_US.UTF-8"));
    map = tmp;
    map.rule(boost::locale::boundary::sentence_term);

    current = map.begin();
    last = map.end();
    last_offset = content.size()-1;

    return;
  }

  // get current sentence's start offset
  int getStartOffset() {
      return startoffset;
  }

  // get current sentence's end offset
  int getEndOffset() {
    return endoffset;
  }

  // get to the next 'passage' (next sentence)
  int next() {
      if (endoffset >= last_offset) {
          return 0;
      }
      ++current;
      std::string::const_iterator this_offset = *current;

      startoffset = endoffset + 1;
      endoffset = this_offset - content_->begin() - 1;
      return 1; // Success
  }

  // get to the next 'passage' contains offset
  int next(int offset) {
      if (offset >=last_offset) {
          return 0;
      }
      current = map.find(content_->begin() + offset);
      std::string::const_iterator this_offset = *current;
      
      endoffset = this_offset - content_->begin() - 1;
      current--;
      std::string::const_iterator tmp_offset = *current;
      startoffset = tmp_offset - content_->begin() - 1 + 1;
      current++;
      return 1; // Success
  }

  const std::string * content_;

 private:
  boost::locale::generator gen;
  boost::locale::boundary::sboundary_point_index map;    //TODO extra copy operation
  boost::locale::boundary::sboundary_point_index::iterator current; 
  boost::locale::boundary::sboundary_point_index::iterator last; 
  int startoffset;
  int endoffset;
  int last_offset;
};

class UnifiedHighlighter {

    public: 
        QQSearchEngine * engine_;   // TODO get reference not object
         
        UnifiedHighlighter();
        UnifiedHighlighter(QQSearchEngine & engine_);
        
        // highlight:
        // primary method: Return snippets generated for the top-ranked documents
        // query: terms of this query
        // topDocs: array of docID of top-ranked documents
        std::vector<std::string> highlight(const Query & query, 
                                           const TopDocs & topDocs, 
                                           const int & maxPassages);

        // highlight each document
        std::string highlightForDoc(const Query & query, 
                                    const int & docID, 
                                    const int & maxPassages);  
        // get each query term's offsets iterator
        OffsetsEnums getOffsetsEnums(const Query & query, const int & docID);  
        // highlight using the iterators and the content of the document
        std::string highlightOffsetsEnums(const OffsetsEnums & offsetsEnums, 
                                          const int & docID, 
                                          const int & maxPassages);  
       
        // helper for generating key for search in cache
        std::string construct_key(const Query & query, const int & docID); 
    
        float passage_norm(const int & start_offset);
        float tf_norm(const int & freq, const int & passageLen);
        
        // highlight based on precomputed scores:
        // primary method highlightQuickForDoc 
        // highlight each document assisted by precomputed scores
        std::string highlightQuickForDoc(const Query & query, 
                                         const int & docID, 
                                         const int &maxPassages);  
        // Helper function for precomputation based snippet generating
        std::vector<int> get_top_passages(const ScoresEnums & scoresEnums, 
                                          const int & maxPassages); 
        ScoresEnums get_passages_scoresEnums(const Query & query, 
                                             const int & docID);
        std::string highlight_passages(const Query & query, 
                                       const int & docID, 
                                       const std::vector<int> & top_passages); 

    private:
        // cache for snippets ( docID+query -> string )
        cache::lru_cache<std::string, std::string> _snippets_cache_ 
          {cache::lru_cache<std::string, std::string>(SNIPPETS_CACHE_SIZE)};
        // cache for on-flash snippets (docID+query -> position(int) of _snippets_store_)
        cache::lru_flash_cache<std::string, std::string> _snippets_cache_flash_ 
          {cache::lru_flash_cache<std::string, std::string>
          (SNIPPETS_CACHE_ON_FLASH_SIZE, SNIPPETS_CACHE_ON_FLASH_FILE)};

        // passage normalization parameters for scoring
        float pivot = 87;  // hard-coded average length of passage(according to Lucene)
        float k1 = 1.2;    // BM25 parameters
        float b = 0.75;    // BM25 parameters
        // passage normalization functions for scoring
        //float passage_norm(int & start_offset);
        //float tf_norm(int freq, int passageLen);
}; 


#endif
