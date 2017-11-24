#include "utils.h"

#include <iostream>
#include <sstream>
#include <iterator>

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

const std::vector<std::string> explode_by_non_letter(const std::string &text) {
    std::istringstream iss(text);
    std::vector<std::string> results((std::istream_iterator<std::string>(iss)),
                                     std::istream_iterator<std::string>());
    return results;
} 


///////////////////////////////////////////////
// LineDoc
///////////////////////////////////////////////

LineDoc::LineDoc(std::string path): infile_(path) {
    // The first line must be header
    std::string line; 
    std::getline(infile_, line);
    std::vector<std::string> items = explode(line, '#');
    items = explode_by_non_letter(items.back());
    col_names_ = items;
}

bool LineDoc::GetRow(std::vector<std::string> &items) {
    std::string line;
    auto &ret = std::getline(infile_, line);
    
    if (ret) {
        items = explode(line, '\t');
        return true;
    } else {
        return false;
    }
}





} // namespace util
