#ifndef HIGHLIGHTER_H
#define HIGHLIGHTER_H

#include <queue>
#include <math.h> 
#include <fstream>
#include <algorithm>

#include <boost/locale.hpp>

#include "engine_services.h"
#include "types.h"
#include "lrucache.h"


std::locale create_locale();

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

  Offset_Iterator(const OffsetPairs & offsets_in) {
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
  const OffsetPairs * offsets;
  OffsetPairs::const_iterator cur_position;
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

    OffsetPairs matches = {};
    std::string to_string(const std::string * doc_string) {
      std::string res= "";
      res += doc_string->substr(startoffset, endoffset - startoffset + 1) + "\n";
      //std::cout << "\nprepare to string: " << res << "\n(" << res.size() << ")" << std::endl;
      // highlight

      auto cmp_terms = [] (OffsetPair & a, OffsetPair & b) -> bool { return (std::get<0>(a) > std::get<0>(b));};
      std::sort(matches.begin(), matches.end(), cmp_terms);
      //std::cout << matches.size() << std::endl;
      for (auto it = matches.begin(); it != matches.end(); it++) {
        //std::cout << "insert end to " << std::get<1>(*it)-startoffset+1 << std::endl;
        res.insert(std::get<1>(*it)-startoffset+1, "<\\b>");
        //std::cout << "insert begin to " << std::max(0, std::get<0>(*it)-startoffset) << std::endl;
        res.insert(std::max(0, std::get<0>(*it)-startoffset), "<b>");
        //res.insert(std::get<0>(*it)-startoffset, "<b>");
      }
      //std::cout << std::endl << res;
      return res;
    }
};

class SentenceBreakIteratorNew {
 public:
  SentenceBreakIteratorNew(const std::string &content, const std::locale & locale) {
    startoffset = endoffset = -1;
    content_ = & content;
    
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
      
      startoffset = endoffset + 1;
      for (endoffset++; endoffset < last_offset; endoffset++) {
          char this_char = (*content_)[endoffset];
          if (this_char == '?' || this_char == '!') {
              break;
          }
          if (this_char == '.') {
              if (endoffset - startoffset > 3) {  // not short
                  if ((*content_)[endoffset-3] != ' ' && (*content_)[endoffset-2] != '.') { // last word not short or U.S.
                      while (endoffset < last_offset) {
                          if ((*content_)[endoffset+1] != ' ') break;  // TODO change to not numbre or chars
                          endoffset++;
                      }
                      if (isupper((*content_)[endoffset+1])) // next char should be upper
                          break;
                  }
              }
          }
      }
      
      while (endoffset < last_offset) {
          if ((*content_)[endoffset+1] != ' ') break;   // TODO change to not number or chars
          endoffset++;
      }
      
      return 1; // Success
  }

  // get to the next 'passage' contains offset
  int next(int offset) {
      if (offset > last_offset) {
          return 0;
      }
      for (endoffset = offset; endoffset < last_offset; endoffset++) {
          if ((*content_)[endoffset] == '.')    //TODO
              break;
      }
      for (startoffset = std::max(0, offset - 1);  startoffset > 0; startoffset--) {
          if ((*content_)[startoffset] == '.') { //TODO
              startoffset++;
              break;
          }
      }
      return 1; // Success
  }


  const std::string * content_;
  int startoffset;
  int endoffset;
 private:
  int last_offset;
};


class SentenceBreakIterator {
 public:
  /*
  SentenceBreakIterator(std::locale &locale): locale_(locale) {
    const std::string & content("");
    map.rule(boost::locale::boundary::sentence_term);
  }
   void set_content(const std::string & content) {
    startoffset = endoffset = -1;
    content_ = & content;
    
    // start boost boundary analysis
    map.map(boost::locale::boundary::sentence, content.begin(), content.end(),
                                                       locale_);
    map.rule(boost::locale::boundary::sentence_term);

    current = map.begin();
    last = map.end();
    last_offset = content.size()-1;
    return;
  }

 */

  SentenceBreakIterator(const std::string &content, const std::locale & locale) {
    startoffset = endoffset = -1;
    content_ = & content;
    // start boost boundary analysis
    //map.reset(new boost::locale::boundary::sboundary_point_index(boost::locale::boundary::sentence, content.begin(), content.end(), locale));
    
    map.map(boost::locale::boundary::sentence, content.begin(), content.end(), locale);
    map.rule(boost::locale::boundary::sentence_term);
    current = map.begin();
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
      if (offset > last_offset) {
          return 0;
      }
      //std::cout <<  last_offset << " last time: " << startoffset << ", " << endoffset; 
      current = map.find(content_->begin() + offset);
      
      if (offset == 0)
          current++; 
      //current = map.find(content_->begin() + offset + 1);
      std::string::const_iterator this_offset = *current;
      
      endoffset = std::max(0, (int)(this_offset - content_->begin() - 1));
      
      current--;
      std::string::const_iterator tmp_offset = *current;
      startoffset = tmp_offset - content_->begin() - 1 + 1;
      //std::cout << "now      : " << startoffset << ", " << endoffset << std::endl; 
      current++;
      return 1; // Success
  }


  const std::string * content_;
  int startoffset;
  int endoffset;
 private:
  boost::locale::boundary::sboundary_point_index map;
  //std::unique_ptr<boost::locale::boundary::sboundary_point_index> map;
  boost::locale::boundary::sboundary_point_index::iterator current; 
  //boost::locale::boundary::sboundary_point_index::iterator last; 
  //std::locale locale_;
  int last_offset;
};


class SimpleHighlighter {
 public:
  SimpleHighlighter() {
    locale_ = create_locale();
  }

  std::string highlightOffsetsEnums(const OffsetsEnums & offsetsEnums, 
                                    const int & maxPassages, 
                                    const std::string &doc_str) {
    if (offsetsEnums.size() == 0) return "";
    // break the document according to sentence
    //std::cout << doc_str << std::endl;
    SentenceBreakIteratorNew breakiterator(doc_str, locale_);
    
    // "merge sorting" to calculate all sentence's score
    
    // priority queue for Offset_Iterator TODO extra copy
    /*auto comp_offset = [] (Offset_Iterator * & a, Offset_Iterator * & b) -> bool { return a->startoffset > b->startoffset; };
    std::priority_queue<Offset_Iterator *, std::vector<Offset_Iterator *>, decltype(comp_offset)> offsets_queue(comp_offset);
    for (auto it = offsetsEnums.begin(); it != offsetsEnums.end(); it++ ) {
      offsets_queue.push(&(*it));
    }*/
    auto comp_offset = [] (Offset_Iterator & a, Offset_Iterator & b) -> bool { return a.startoffset > b.startoffset; };
    std::priority_queue<Offset_Iterator, std::vector<Offset_Iterator>, decltype(comp_offset)> offsets_queue(comp_offset);
    for (auto it = offsetsEnums.begin(); it != offsetsEnums.end(); it++ ) {
      offsets_queue.push(*it);
    }

    // priority queue for passages
    auto comp_passage = [] (Passage * & a, Passage * & b) -> bool { return a->score > b->score; };
    std::priority_queue<Passage *, std::vector<Passage*>, decltype(comp_passage)> passage_queue(comp_passage);
    float min_score = -1;

    // start caculate score for passage
    Passage * passage = new Passage();  // current passage
    while (!offsets_queue.empty()) {
      // analyze current passage
      Offset_Iterator cur_iter = offsets_queue.top();
      offsets_queue.pop();

      int cur_start = cur_iter.startoffset;
      // judge whether this iterator is over
      if (cur_start == -1)
        continue;
      int cur_end = cur_iter.endoffset;
      //std::cout <<"cur offset: " << cur_start << "," << cur_end << std::endl;

      // judge whether this iterator's offset is beyond current passage
      //if (cur_start > passage->endoffset) {
      if (cur_end > passage->endoffset) {
        // if this passage is not empty, then wrap up it and push it to the priority queue
        if (passage->startoffset >= 0) {
          passage->score = passage->score * passage_norm(passage->startoffset); //normalize according to passage's startoffset
          //if (passage_queue.size() == maxPassages && passage->score <= passage_queue.top()->score) {
          if (passage_queue.size() == maxPassages && passage->score <= min_score) {
            passage->reset();
          } else {
            passage_queue.push(passage);
            if (passage_queue.size() > maxPassages) {
              passage = passage_queue.top();
              passage_queue.pop();
              passage->reset();
            } else {
              passage = new Passage();
            }
            min_score = passage_queue.top()->score;
          }
        }
        // advance to next passage
        if (breakiterator.next(cur_end) <= 0) {
            break;
        }
        //passage->startoffset = breakiterator.getStartOffset();
        //passage->endoffset = breakiterator.getEndOffset();
        passage->startoffset = breakiterator.startoffset;
        passage->endoffset = breakiterator.endoffset;
        //std::cout << "next passage: " << breakiterator.startoffset << " -> " << breakiterator.endoffset << std::endl;
      }

      // Add this term's appearance to current passage until out of this passage
      int tf = 0;
      while (1) {
        tf++;
        passage->addMatch(cur_start, cur_end); 
        cur_iter.next_position();
        if (cur_iter.startoffset == -1) {
          break;
        }
        cur_start = cur_iter.startoffset;
        cur_end = cur_iter.endoffset;
        //if (cur_start > passage->endoffset) {
        if (cur_end > passage->endoffset) {
          offsets_queue.push(cur_iter);
          break;
        }
        //cur_end = cur_iter.endoffset;
      }
      // Add the score from this term to this passage
      passage->score = passage->score + cur_iter.weight * tf_norm(tf, passage->endoffset-passage->startoffset+1);   //scoring
    }

    // Add the last passage
    passage->score = passage->score * passage_norm(passage->startoffset);
    if (passage->score > 0) {
    //if (passage_queue.size() < maxPassages && passage->score > 0) {
    if (passage_queue.size() < maxPassages) {
      passage_queue.push(passage);
    } else {
      //if (passage->score > passage_queue.top()->score) {
      if (passage->score > min_score) {
        Passage * tmp = passage_queue.top();
        passage_queue.pop();
        delete tmp;
        passage_queue.push(passage);
      }
    }
    }
    // format it into a string to return
    std::vector<Passage *> passage_vector;
    while (!passage_queue.empty()) {
      passage_vector.push_back(passage_queue.top());
      passage_queue.pop();
    }
    // sort array according to startoffset
    auto comp_passage_offset = [] (Passage * & a, Passage * & b) -> bool { return a->startoffset < b->startoffset; };
    std::sort(passage_vector.begin(), passage_vector.end(), comp_passage_offset);


    // format the final string
    std::string res = "";
    for (auto it = passage_vector.begin(); it != passage_vector.end(); it++) {
      res += (*it)->to_string(breakiterator.content_);   // highlight appeared terms
      //std::cout <<  "("  << (*it)->score << ") " << breakiterator.content_->substr((*it)->startoffset, (*it)->endoffset - (*it)->startoffset+1) << std::endl;
      delete (*it);
    }
    //std::cout << res << std::endl;
    return res;
  }

  float passage_norm(const int & start_offset) {
      return 1 + 1/(float) log((float)(pivot + start_offset));
  }

  float tf_norm(const int & freq, const int & passageLen) {
      float norm = k1 * ((1 - b) + b * (passageLen / pivot));
      return freq / (freq + norm);
  }


 protected:
  //std::unique_ptr<SentenceBreakIterator> breakiterator;
  float pivot = 87;  // hard-coded average length of passage(according to Lucene)
  float k1 = 1.2;    // BM25 parameters
  float b = 0.75;    // BM25 parameters

  std::locale locale_;        // all generator
};


#endif
