import sys

def do_parse(querylog, output):
    # parse each line
    output.write('<parameters>\n')
    count = 0
    for line in querylog:
        output.write('  <query>\n')
        output.write('    <type>indri</type>\n')
        output.write('    <type>'+str(count)+'</type>\n')
        output.write('    <text>\n')
        line = line.replace('AND', ' ')
        line = line.replace('\"', '')
        line = line.replace('\n', '')
        output.write('      '+line+'\n')
        output.write('    </text>\n')
        output.write('  </query>\n')
        count += 1
    output.write('</parameters>\n')

if __name__=='__main__':
    # print help
    if len(sys.argv)!=3:
        print('Usage: python parse_querylog_4_indri.py [querylog_name] [output_name]')
        exit(1)

    
    # do parse
    querylog = open(sys.argv[1])
    output = open(sys.argv[2], 'w')


    do_parse(querylog, output)


