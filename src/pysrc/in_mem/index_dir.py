from search_engine import *


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

