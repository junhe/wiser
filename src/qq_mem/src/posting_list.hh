#ifndef POSTING_LIST_HH
#define POSTING_LIST_HH

#include <string>

class Posting {
    public:
        int docID;
        int term_frequency;
        int * position;

        Posting * next;

        Posting(int docID_in, int term_frequency, int num_pos, int position[]);
        std::string dump();
};


class Posting_List {
    std::string term;
    Posting * p_list;
    
    public:
        Posting_List(std::string term_in);
        void add_doc(int docID, int term_frequency, int num_pos, int position[]);
        std::string serialize();
};


#endif
