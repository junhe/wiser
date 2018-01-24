import sys
import re
import string

def do_clean(input_file, output): 
    content = str(input_file.read())
    content = re.sub(r'<.*>','', content)
    output.write(content)

if __name__=='__main__':
    # print help
    if len(sys.argv) != 2:
        print('Usage: python clean.py [input_name] (output will be input_name_clean)')
        exit(1)

    
    # do parse
    input_file = open(sys.argv[1])
    output = open(sys.argv[1]+'_clean', 'w')

    do_clean(input_file, output)
    output.close()
    input_file.close()


