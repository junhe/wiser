#include <string>
#include <vector>

typedef std::string Doc;
typedef std::string Snippet;
typedef std::string Term;
typedef std::vector<Term> TermList;

Snippet highlighter(Doc doc, TermList query);
