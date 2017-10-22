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
        terms is a list of terms that are in a document,
        for example, ['hello', 'world', 'good', 'bad']
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
        # Use try block to reduce dictionary search
        if self.inverted_index.has_key(term):
            return self.inverted_index[term].get_doc_id_set()
        else:
            return set([])

    def search(self, terms, operator):
        """
        example:
            terms = ["hello", "world"]
            operator = 'AND'

        it returns a list of document IDs that matches the search query
        """
        if operator != "AND":
            raise NotImplementedError("We only support AND operator at this moment")

        if operator == "AND":
            return self.search_and(terms)

    def search_and(self, terms):
        doc_id_sets = []
        for term in terms:
            doc_id_set = self.get_doc_id_set(term)
            doc_id_sets.append(doc_id_set)

        if len(doc_id_sets) == 0:
            return []

        intersected_docs = reduce(lambda x, y: x & y, doc_id_sets)
        return list(intersected_docs)



class IndexWriter(object):
    def __init__(self, index, doc_store, tokenizer):
        self.index = index
        self.doc_store = doc_store
        self.tokenizer = tokenizer

    def add_doc(self, doc_dict):
        """
        Index all the fields of the doc_dict
        """
        doc_id = self.doc_store.add_doc(doc_dict)

        for k, v in doc_dict.items():
            terms = self.tokenizer.tokenize(str(v))
            self.index.add_doc(doc_id, terms)


class Tokenizer(object):
    """
    Use this naive tokenizer for now. Third-party tokenizers are available.
    """
    def __init__(self):
        pass

    def tokenize(self, text):
        return text.lower().split()


class Searcher(object):
    def __init__(self, index, doc_store):
        self.index = index
        self.doc_store = doc_store

    def search(self, terms, operator):
        terms = [term.lower() for term in terms]
        doc_ids = self.index.search(terms, operator)

        return doc_ids

    def retrieve_docs(self, doc_ids):
        docs = []
        for doc_id in doc_ids:
            doc = self.doc_store.get_doc(doc_id)
            docs.append(doc)

        return docs


def build_index_of_directory(index_writer, input_dir):
    for fname in os.listdir(input_dir):
        cur_file = open(os.path.join(input_dir, fname))
        text = str(cur_file.read())
        doc_dict = {
                "filename": fname,
                "text": text
                }

        index_writer.add_doc(doc_dict)

        cur_file.close()


if __name__=='__main__':
    # print help
    if len(sys.argv)!=2:
        print('========== Usage: python search_engine.py [index_dir]')
        exit(1)
    input_dir = sys.argv[1]

    index = Index()
    doc_store = DocStore()
    tokenizer = Tokenizer()

    index_writer = IndexWriter(index, doc_store, tokenizer)
    searcher = Searcher(index, doc_store)

    build_index_of_directory(index_writer, input_dir)

    # interactive query
    while(True):
        print('========== ready for searching:')
        query = raw_input('Prompt ("stop" to quit): ')
        if query == 'stop' or query == 'quit':
            exit(0)
        doc_ids = searcher.search([query], "AND")
        docs = searcher.retrieve_docs(doc_ids)
        print docs

