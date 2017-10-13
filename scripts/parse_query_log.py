import os
import sys
import re

def do_parse(input_file, output_file):
    text = str(input_file.read())
    query_content = re.findall(r'\t.*\t',text)
    for each_query in query_content:
        # clean
        each_query = each_query.replace('\t', '')
        # extract all words from this query
        each_query = each_query.replace('+', ' ')
        output_file.write(each_query+'\n')

if __name__=='__main__':
    # print help
    if len(sys.argv)!=3:
        print('Usage: python parse_query_log.py [input_name] [output_name]')
        exit(1)

    # do parse
    input_file = open(sys.argv[1])
    output_file = open(sys.argv[2],'w')

    do_parse(input_file, output_file)
    output_file.close()
