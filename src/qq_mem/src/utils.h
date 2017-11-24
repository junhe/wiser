#ifndef UTILS_H
#define UTILS_H

#include <string>
#include <vector>
#include <fstream>


namespace utils {

const std::vector<std::string> explode(const std::string& s, const char& c);
const std::vector<std::string> explode_by_non_letter(const std::string& text);
const std::vector<std::string> explode_strict(const std::string& s, const char& c);

class LineDoc {
    private:
        std::ifstream infile_;
        std::vector<std::string> col_names_;

    public:
        LineDoc(std::string path);
        bool GetRow(std::vector<std::string> &items);
};


} // namespace util
#endif

