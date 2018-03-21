#include <iostream>
#include <string>
#include <fstream>
#include <sstream>

using namespace std;

class SentenceBreakIteratorNew {
 public:
  SentenceBreakIteratorNew(const std::string &content) {
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
          if ((*content_)[endoffset] == '.')
              break;
      }
      for (startoffset = std::max(0, offset - 1);  startoffset > 0; startoffset--) {
          if ((*content_)[startoffset] == '.') {
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

int main() {
    //string str = "how soft works?Java!Python";
    std::ifstream infile_;
    infile_.open("/mnt/ssd/wiki/sample_docs");
    string str;
    while (getline(infile_, str)){
    //cout << "=============" << endl;
    SentenceBreakIteratorNew itor(str);
    int start_offset, end_offset;
    while ( itor.next() > 0 ) {
        start_offset = itor.getStartOffset();
        end_offset = itor.getEndOffset();
        cout << start_offset << "," << end_offset << ";";
        //cout << "\t*" << str.substr(start_offset, end_offset-start_offset+1) << "*" << endl;
    }
    cout << endl;
    }
}
