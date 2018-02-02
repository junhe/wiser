import sys
import re
import string

def do_generate(input_file, output): 
    content = str(input_file.read())
    # replace <doc> and next line with next line
    #pattern = re.compile(r'<doc.*>\n.*\n')
    #content = str(pattern.findall(content))
    documents = re.split(r'</doc>\n', content)
    content = ''
    for document in documents:
        #print "Here: ", document
        document = re.sub(r'<doc.*>\n','', document)
        document = re.sub(r'\n', '\t', document, count=1)
        document = re.sub(r'\n', ' ', document)
        content += document + '\n'
    #content = re.sub(r'<doc.*>\n', '', title_extract.group(0))
    #content = re.sub(r'\n', '\t', content)
    # replace </doc> with next 
    # replace </doc>  with \n
    output.write(content)

if __name__=='__main__':
    # print help
    if len(sys.argv) != 2:
        print('Usage: python generate_linedoc.py [input_name] (output will be input_name_linedoc)')
        exit(1)

    
    # do parse
    input_file = open(sys.argv[1])
    output = open(sys.argv[1]+'_linedoc', 'w')

    do_generate(input_file, output)
    output.close()
    input_file.close()


