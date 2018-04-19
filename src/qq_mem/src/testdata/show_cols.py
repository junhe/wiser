
#doctitle        body    tokenized       offsets positions
TITLE = 0
BODY = 1
TOKENIZED = 2
OFFSETS = 3
POSITIONS = 4

with open("./line_doc_with_positions") as f:
    index = 0
    for line in f:
        print "----- Index: ", index, " --------------"
        print "body: ", line.split("\t")[BODY][0:100]
        print "tokenized: ", line.split("\t")[TOKENIZED][0:100]
        index += 1

