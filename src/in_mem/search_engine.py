import os
import sys
import re
import readline


class PostingList(object):
    def __init__(self):
        """
        postinglist
        key: doc_id, value: { }
        for example:
        posting_list = {
            88: {'frequency': 3, 'page_rank': 83},
            ...
        }
        """
        self.postinglist = {}

    def update_posting(self, doc_id, payload_dict):
        """
        If doc_id does not exist, it will be added.
        """
        if self.postinglist.has_key(doc_id):
            self.postinglist[doc_id].update(payload_dict)
        else:
            self.postinglist[doc_id] = payload_dict

    def get_doc_id_set(self):
        return set(self.postinglist.keys())

    def get_payload_dict(self, doc_id):
        return self.postinglist[doc_id]

    def dump(self):
        return self.postinglist


class DocStore(object):
    """
    This is where the documents are stored. Documents are stored as dictionaries
    """
    def __init__(self):
        self.docs = {}
        self.next_doc_id = 0

    def add_doc(self, doc_dict):
        doc_id = self.next_doc_id
        self.next_doc_id += 1

        self.docs[doc_id] = doc_dict

        return doc_id

    def get_doc(self, doc_id):
        return self.docs[doc_id]


class Index(object):
    def __init__(self):
        """
        The key in inverted_index is the term, the value is class PostingList.
        """
        self.inverted_index = {}

    def add_doc(self, doc_id, terms):
        """
        terms is a list of terms, for example, ['hello', 'world', 'good', 'bad']
        """
        for term in terms:
            if self.inverted_index.has_key(term):
                postinglist = self.inverted_index[term]
            else:
                postinglist = PostingList()
                self.inverted_index[term] = postinglist
            postinglist.update_posting(doc_id, {})

    def get_postinglist(self, term):
        return self.inverted_index.get(term, None)

    def get_doc_id_set(self, term):
        return self.inverted_index[term].get_doc_id_set()


class OldIndex(object):

    # Main data structure: dic(item->PostingList)   (dictionary is used for one convenient hash table)
    #                      PostingList: list or directory of Posting(doc_id, frequency)

    # Core function: build_index(), from all files in the input_dir

    def __init__(self, input_dir):
        self.input_dir = input_dir
        self.init_index()
        self.build_index()

    def init_index(self):
        # create a hash table (term->PostingList(list[posting]))
        self.invertedindex = {}
        # create a hash table (doc_id -> doc_name)
        self.doc = {}
        # doc counts
        self.doc_num = 0

    def build_index(self):
        print('========== building index from' + self.input_dir)
        # for each term in each doc, update the hash table
        for fname in os.listdir(input_dir):
            # store doc_id->doc_name
            self.doc[self.doc_num] = fname
            self.doc_num += 1

            cur_file = open(input_dir+fname)
            text = str(cur_file.read())
            for item in text.split():
                # update invertedindex
                if item not in self.invertedindex:
                    self.invertedindex[item] = PostingList()
                self.invertedindex[item].add_posting(self.doc_num-1, 1)
            cur_file.close()
        print('========== finished indexing')


    # Tool functions: dump; postings(get a PostingList of an item); doc_id_2_docname(transform doc_id to docname)
    def dump(self):
        print('========== dumping inverted index:')
        print('overall files: ' + str(self.doc_num))
        print('overall items: ' + str(len(self.invertedindex)))
        for postinglist in self.invertedindex.iteritems():
            print (postinglist[0] + ': ')
            print ('\t' + str(postinglist[1].dump()))

    def postings(self, item):
        if item in self.invertedindex:
            return self.invertedindex[item].dump().keys()  # only return doc_id
        return {}

    def doc_id_2_docname(self, doc_set):
        return map(lambda x:self.doc[x], doc_set)

class Searcher(object):

    # Init: bind searcher to an in-mem index
    def __init__(self, index):
        self.index = index


    # Main function: query AND logic  -> return list of docname
    def searchAND(self, phrase):
        items = list(set(phrase.split()))     #simple analysis
        print('querying '+ str(items))

        flag = True
        for item in items:
            if flag:
                result = set(self.index.postings(item))
                flag = False
            result = result & set(self.index.postings(item))

        return self.index.doc_id_2_docname(result)


if __name__=='__main__':
    # print help
    if len(sys.argv)!=2:
        print('========== Usage: python search_engine.py [index_dir]')
        exit(1)
    input_dir = sys.argv[1]

    # create index
    in_mem_index = Index(input_dir)
    # in_mem_index.dump()


    # create searcher
    in_mem_searcher = Searcher(in_mem_index)


    # interactive query
    while(True):
        print('========== ready for searching:')
        query = raw_input('Prompt ("stop" to quit): ')
        if query == 'stop' or query == 'quit':
            exit(0)
        print('in_files: ' + str(in_mem_searcher.searchAND(query)))

