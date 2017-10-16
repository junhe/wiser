import os
import sys
import re
import urllib

from subprocess import call

def parse_AOL_file(input_file, output_file):
    text = str(input_file.read())
    query_content = re.findall(r'\t.*2006-',text)
    for each_query in query_content:
        # clean
        each_query = each_query.replace('\t', '\"')
        each_query = each_query.replace('2006-', '')
        # extract items sequence from this string
        each_query = each_query.replace(' ', '\" AND \"')
        output_file.write(each_query+'\n')

def parse_wiki_file(input_file, output_file):
    text = str(input_file.read())
    # only search related requests & only english wiki
    query_content = re.findall(r'en\.wikipedia\.org.*search=.*-',text) + re.findall(r'www\.wikipedia\.org.*search=.*\&language=en',text)
    for each_query in query_content:
        # clean
            # get the search area
        each_query = re.sub(r'.*search=', '\"', each_query)
        each_query = re.sub(r'&.*-', ' -', each_query)
        each_query = each_query.replace(' -', '\"')
            # translate url encoding letters non-letter
        each_query = re.sub(r'.*%[\d\w]{2}.*', '', each_query)
            #TODO urllib.unquote(each_query)
        # extract items sequence from this string
        # TODO different from AOL, may need analyzer's analysis
        each_query = each_query.replace('+', '\" AND \"')
        if each_query != '':
            #print each_query
            output_file.write(each_query+'\n')

def do_parse(input_file, output_file):
    text = str(input_file.read())
    query_content = re.findall(r'\t.*\t',text)
    for each_query in query_content:
        # clean
        each_query = each_query.replace('\t', '\"')
        # extract all words from this query
        each_query = each_query.replace('+', '\" AND \"')
        output_file.write(each_query+'\n')

if __name__=='__main__':
    # print help
    if len(sys.argv)!=4:
        print('Usage: python parse_query_log.py [type:AOL, wikitrace] [input_dir] [output_name]')
        exit(1)

    
    # do parse
    log_type = sys.argv[1]
    input_dir = sys.argv[2]
    output_file = open(sys.argv[3],'w')

    # parse according to different types
    if log_type == 'AOL':
        print('parsing query log from AOL')
        # parsing all .txt files under input directory
        for fname in os.listdir(input_dir):
            input_file = open(input_dir + fname)
            parse_AOL_file(input_file, output_file)
            input_file.close()
    
    elif log_type == 'wikitrace':
        print('parsing query log from wikitrace')
        # parsing all .gz files under input directory
        zipfiles = [] 
        for fname in os.listdir(input_dir):
            # get all gz file names
            zipfiles.append(fname)
        print zipfiles
        for zipname in zipfiles:
            # decompress this .gz file
            print('decompressing ' + zipname)
            call(['gunzip', '-d', zipname])
            # parse related wiki file
            fname = zipname.replace('.gz', '')
            print('get uncompressed wiki trace: ' + fname)
            input_file = open(input_dir + fname)
            parse_wiki_file(input_file, output_file)
            input_file.close()
            # delete this wiki file
            call(['rm' , fname])
    else:
        print('wrong type: AOL or wikitrace only')
        exit(1)
    
    output_file.close()
