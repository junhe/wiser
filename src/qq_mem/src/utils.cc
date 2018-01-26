#include "utils.h"

#include <iostream>
#include <iomanip>
#include <thread>
#include <iterator>
#include <cassert>
#include <stdexcept>
#include <thread>
#include <queue>
#include <utility>
#include <locale>


namespace utils {

// To split a string to vector
// Not sure how good it is for unicode...
const std::vector<std::string> explode(const std::string& s, const char& c)
{
    std::string buff{""};
    std::vector<std::string> v;
    
    for(auto n:s)
    {
        if(n != c) buff+=n; else
        if(n == c && buff != "") { v.push_back(buff); buff = ""; }
    }
    if(buff != "") v.push_back(buff);
    
    return v;
}

// TODO: Replace this boost tokenizer
const std::vector<std::string> explode_by_non_letter(const std::string &text) {
    std::istringstream iss(text);
    std::vector<std::string> results((std::istream_iterator<std::string>(iss)),
                                     std::istream_iterator<std::string>());
    return results;
} 

const std::vector<std::string> explode_strict(const std::string &line, const char& sep) {
    std::string buff{""};
    std::vector<std::string> vec;
    
    for(auto ch:line)
    {
        if(ch != sep) {
            buff += ch; 
        } else { 
            vec.push_back(buff); 
            buff = "";
        }
    }
    vec.push_back(buff);
    
    return vec;
}

void sleep(int n_secs) {
  using namespace std::this_thread; // sleep_for, sleep_until
  using namespace std::chrono; // nanoseconds, system_clock, seconds

  sleep_for(seconds(n_secs));
}

const time_point now() {
    return std::chrono::system_clock::now();
}

const double duration(std::chrono::time_point<std::chrono::system_clock> t1, 
        std::chrono::time_point<std::chrono::system_clock> t2) {
    std::chrono::duration<double> diff = t2 - t1;
    return diff.count();
}

// Note that width is the final length of the string, not
// the number of zeros.
const std::string fill_zeros(const std::string &s, std::size_t width) {
    int n_zeros = width - s.size();
    if (n_zeros > 0) {
        return std::string(n_zeros, '0') + s; 
    } else {
        return s;
    }
}

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

const std::string format_double(const double &x, const int &precision) {
  std::ostringstream out;
  out << std::setprecision(precision) << x;
  return out.str();
}


std::vector<DocIdType> find_top_k(const DocScoreVec &doc_scores, int k) {
  std::priority_queue<DocScore> queue(std::less<DocScore>(), doc_scores);
  std::vector<DocIdType> ret;
  
  while (k > 0 && !queue.empty()) {
    ret.push_back(queue.top().doc_id);
    queue.pop();
    k--;
  }

  return ret;
}

// It returns the number of terms in field
int count_terms(const std::string field) {
  return utils::explode(field, ' ').size();
}

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



void add_term_offset_entry(std::map<Term, OffsetPairs> *result, const Term &buf, const int &i) {
  int n = buf.size();
  int end = i - 1;
  int start = end - n + 1;

  OffsetPair offset_pair = std::make_tuple(start, end);

  (*result)[buf].push_back(offset_pair);
}

std::map<Term, OffsetPairs> extract_offset_pairs(const std::string & token_str) {
  const std::size_t n = token_str.size();
  std::string buf = "";
  std::map<Term, OffsetPairs> result;

  int i;
  for (i = 0; i < n; i++) {
    char ch = token_str[i];
    if (ch != ' ') {
      buf += ch;
    } else {
      if (buf != "") {
        add_term_offset_entry(&result, buf, i);
        buf = "";
      }
    }
  }

  if (buf != "") {
    add_term_offset_entry(&result, buf, i);
  }

  return result;
}

std::map<Term, OffsetPairs> construct_offset_pairs(const TermList & tokens, 
                                                   const std::vector<Offsets> & token_offsets) {
  std::map<Term, OffsetPairs> result;
  for (int i = 0; i < tokens.size(); i++) {
    result[tokens[i]] = token_offsets[i];
  }
  return result;
}


std::string str_qq_search_reply(const qq::SearchReply &reply) {
  std::ostringstream oss;
  oss << "Size: " << reply.entries_size() ;
  for (int i = 0; i < reply.entries_size(); i++) {
    oss << " doc_id: " << reply.entries(i).doc_id() 
      << " snippet: " << reply.entries(i).snippet();
  }
  return oss.str(); 
}



// buf must be at least 5 bytes long, which is the size required
// to store a full unsigned int
// It returns the number of bytes stored in buf
int varint_encode(uint32_t value, std::string *buf, int offset) {
  int i = 0;
  // inv: value has what left to be encoded
  //      buf[offset, offset+i) has encoded bytes
  //      buf[offset + i] is the location to put next byte
  //      i is the number of encoded bytes
  while (i == 0 || value > 0) {
    (*buf)[offset + i] = (value & 0x7f) | 0x80; // always set MSB
    value >>= 7;
    i++;
  }
  (*buf)[offset + i - 1] &= 0x7f;
  return i;
}


// from varint code to int
// return: length of the buffer decoded
int varint_decode(const std::string &buf, int offset, uint32_t *value) {
  int i = 0;
  *value = 0;
  // inv: buf[offset, offset + i) has been copied to value 
  //      (buf[offset + i] is about to be copied)
  while (i == 0 || (buf[offset + i - 1] & 0x80) > 0) {
    *value += (0x7f & buf[offset + i]) << (i * 7);
    i++;
  }
  return i; 
}



} // namespace util
