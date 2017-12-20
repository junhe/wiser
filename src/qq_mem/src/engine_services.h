#ifndef ENGINE_SERVICES_H
#define ENGINE_SERVICES_H


#include <string>
#include <vector>
#include <map>
#include <tuple>

// for direct IO
#define PAGE_SIZE 512

// for flash based documents
#define FLAG_DOCUMENTS_ON_FLASH true
#define DOCUMENTS_CACHE_SIZE 100
#define DOCUMENTS_ON_FLASH_FILE "/mnt/ssd/documents_store"

// for flash based postings
#define FLAG_POSTINGS_ON_FLASH true
#define POSTINGS_CACHE_SIZE 100
#define POSTINGS_ON_FLASH_FILE "/mnt/ssd/postings_store"
#define POSTING_SERIALIZATION "cereal"    // cereal or protobuf

// for precomputing of snippets generating
#define FLAG_SNIPPETS_PRECOMPUTE true

// size of snippets cache of highlighter
#define SNIPPETS_CACHE_SIZE 100
#define FLAG_SNIPPETS_CACHE false
#define SNIPPETS_CACHE_ON_FLASH_SIZE 100
#define FLAG_SNIPPETS_CACHE_ON_FLASH false
#define SNIPPETS_CACHE_ON_FLASH_FILE "/mnt/ssd/snippets_store.cache"

class DocumentStoreService {
    public:
        virtual void Add(int id, std::string document) = 0;
        virtual void Remove(int id) = 0;
        virtual const std::string & Get(int id) = 0;
        virtual bool Has(int id) = 0;
        virtual void Clear() = 0;
        virtual int Size() = 0;
};


typedef std::string Term;
typedef std::vector<Term> TermList;
typedef TermList Query;
enum class SearchOperator {AND, OR};

class InvertedIndexService {
    public:
        virtual void AddDocument(const int &doc_id, const TermList &termlist) = 0;
        virtual std::vector<int> GetDocumentIds(const Term &term) = 0;
        virtual std::vector<int> Search(const TermList &terms, const SearchOperator &op) = 0;
};

class PostingService {
    public:
        virtual std::string dump() = 0;
};


// Posting_List Class
class PostingListService {
    public:
// Get size
        virtual std::size_t Size() = 0;
// Get next posting for query processing
        // virtual PostingService GetPosting() = 0;                          // exactly next
        // virtual PostingService GetPosting(const int &next_doc_ID) = 0;    // next Posting whose docID>next_doc_ID
// Add a doc for creating index
        // virtual void AddPosting(int docID, int term_frequency, const Positions positions) = 0;
// Serialize the posting list to store 
        virtual std::string Serialize() = 0;    // serialize the posting list, return a string

};


// for highlighter
typedef std::vector<int> TopDocs;
typedef std::tuple<int, int> Offset;  //startoffset, endoffset

// for add_document
typedef std::vector<Offset> Offsets;

// class Term_With_Offset
class TermWithOffset {
    public:
        Term term_;
        Offsets offsets_;

        TermWithOffset(Term term_in, Offsets offsets_in) : term_(term_in), offsets_(offsets_in) {} 
};
typedef std::vector<TermWithOffset> TermWithOffsetList;

// for precompute (score of each passage of each term)
// for (term, doc)
typedef std::pair<int, float> Passage_Score; // passage->score
typedef std::vector<Passage_Score> Passage_Scores;
typedef std::pair<int, int> Passage_Split; // start offset in offsets, len
// for each doc
typedef std::pair<int, int> Passage_Segment; // startoffset, length
typedef std::vector<Passage_Segment> Passage_Segments;

// File Reader (TODO become a abstract class?)
typedef std::pair<int, int> Store_Segment;    // startoffset, length on flash based file
class FlashReader {
    public:
        FlashReader(const std::string & based_file);
        ~FlashReader();
        std::string read(const Store_Segment & segment);
        Store_Segment append(const std::string & value);

    private:
        std::string _file_name_;
        int _fd_;
        int _last_offset_; // append from it
};

// TODO: this design is not good!
extern FlashReader * Global_Posting_Store; //defined in qq_engine.cc
extern FlashReader * Global_Document_Store; //defined in qq_engine.cc

extern bool flag_posting;
#endif
