#ifndef UTILS_H
#define UTILS_H

#include <chrono>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "engine_services.h"

namespace utils {

const std::vector<std::string> explode(const std::string& s, const char& c);
const std::vector<std::string> explode_by_non_letter(const std::string& text);
const std::vector<std::string> explode_strict(const std::string& s, const char& c);
void sleep(int n_secs);
const std::chrono::time_point<std::chrono::system_clock> now();
const double duration(std::chrono::time_point<std::chrono::system_clock> t1, 
    std::chrono::time_point<std::chrono::system_clock> t2);
const std::string fill_zeros(const std::string &s, std::size_t width);

const std::string format_double(const double &x, const int &precision);

typedef std::map<std::string, std::string> ResultRow;
typedef std::vector<ResultRow> ResultTable;
std::string dump_result_table(ResultTable result_table);
std::vector<DocIdType> find_top_k(const DocScoreVec &doc_scores, int k);

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

