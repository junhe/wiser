
class LineDocPool(object):
    def __init__(self, doc_path):
        self.fd = open(doc_path)

        # Get column names
        # The first line of the doc must be the header
        # Sample header:
        #   FIELDS_HEADER_INDICATOR###      doctitle        docdate body
        header = self.fd.readline()
        self.col_names = header.split("###")[1].split()

    def doc_iterator(self):
        for line in self.fd:
            yield self.line_to_dict(line)

    def line_to_dict(self, line):
        items = line.split("\t")
        return {k:v for k,v in zip(self.col_names, items)}




