


class QueryAnalyer:
    def __init__(self, path):
        self.path = path

    def parse_line(self, line):
        line = line.strip("\"")
        line = line.replace("\"", " ")
        return line.split()

    def extract_terms(self):
        terms = []
        f = open(self.path)
        for line in f:
            terms += self.parse_line(line)

        f.close()

        return terms

    def run(self, path):
        terms = self.extract_terms()
        window = []
        in_pre_win = []
        win_size = 10000

        cnt = 0
        for term in terms:
            if term in window:
                in_pre_win.append(True)
                cnt += 1
            else:
                in_pre_win.append(False)

            window.append(term)
            if len(window) > win_size:
                del window[0]

        print "total terms:", len(terms)
        print "window size:", win_size
        print "in prev window:", cnt
        print "in / all:", cnt / (float)(len(terms))

        # tuples = zip(terms, in_pre_win)
        # self.write(tuples, path)

    def write(self, tuples, path):
        with open(path, "w") as f:
            for term, in_pre in tuples:
                print term, in_pre




def main():
    # analyzer = QueryAnalyer("./sample_queries")
    analyzer = QueryAnalyer("/mnt/ssd/realistic_querylog")
    analyzer.run("./locality.text")

if __name__ == "__main__":
    main()
