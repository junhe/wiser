#ifndef ENGINE_SERVICES_HH
#define ENGINE_SERVICES_HH


#include <string>
#include <vector>

class DocumentStoreService {
    public:
        virtual void Add(int id, std::string document) = 0;
        virtual void Remove(int id) = 0;
        virtual std::string Get(int id) = 0;
        virtual bool Has(int id) = 0;
        virtual void Clear() = 0;
        virtual int Size() = 0;
};

typedef std::vector<std::string> TermList;
enum class SearchOperator {AND, OR};

class InvertedIndexService {
    public:
        virtual void AddDocument(int doc_id, TermList termlist) = 0;
        virtual void GetPostingList(int doc_id) = 0;
        virtual void Search(TermList terms, SearchOperator op) = 0;
};

#endif

