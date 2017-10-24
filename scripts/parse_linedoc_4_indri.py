import sys

def do_parse(linedoc, output):
    # read header
    header = linedoc.readline()
    col_names = header.split("###")[1].split()
   
    # parse each line
    count = 0
    for line in linedoc:
        output.write('<DOC>\n')
        items = line.split("\t")
        count += 1
        terms = zip(col_names, items)[3][1]
        terms = terms.replace(',', ' ')

        output.write('<DOCNO> ' + str(count) + ' </DOCNO>\n')
        output.write('<TEXT>\n')
        output.write(terms + '\n')
        output.write('</TEXT>\n')
        output.write('</DOC>\n')


if __name__=='__main__':
    # print help
    if len(sys.argv)!=3:
        print('Usage: python parse_linedoc_4_indri.py [linedoc_name] [output_name]')
        exit(1)

    
    # do parse
    linedoc = open(sys.argv[1])
    output = open(sys.argv[2], 'w')


    do_parse(linedoc, output)


