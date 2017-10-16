import re
import os
import sys 

from subprocess import call

def counting_analyze(input_file, output_dir):
    text = input_file.readlines()
    f_stat = open('stat.txt', 'w')
    f_popular_queries = open('popular_queries.txt', 'w')
    f_popular_terms = open('popular_terms.txt', 'w')
    f_popular_queries.write('query\t#occurance\t#ratio')
    f_popular_terms.write('term\t#occurance\t#ratio')
    MAX_OUTPUT = 100000  # output how many popular terms/queries

    # term frequency                     done
    # query frequency                    done
    # exact same-as-before queries       done
    # number of terms in queries         done
    # overall terms, unique terms        done
    # overall queries, unique queries    done
    

    # analyze
    query_dic = {}
    term_dic = {}
    term_stat_dic = {}
    n_queries = 0
    n_terms = 0
    n_consecutive_dup = 0
    last_query = ''
    for each_query in text:
        # query related
        n_queries += 1
        # TODO ?delete null terms -> filter work
        each_query = each_query.replace('\n', '')
        query_dic.setdefault(each_query.lower(), 0)
        query_dic[each_query.lower()] += 1
        if each_query == last_query:
            n_consecutive_dup += 1
        last_query = each_query

        # term related
        terms = each_query.split(b'AND')
        term_stat_dic.setdefault(str(len(terms)), 0)
        term_stat_dic[str(len(terms))] += 1
        n_terms += len(terms)
        for term in terms:
            term_dic.setdefault(term.lower(), 0)
            term_dic[term.lower()] += 1
        




    # print query related statistics
    li = sorted(query_dic.iteritems(), key=lambda d:d[1], reverse = True)
    count = 0
    f_stat.write('\noverall queries               : ' + str(n_queries))
    f_stat.write('\nunique queries                : ' + str(len(query_dic)))
    f_stat.write('\nexactly-same-as-before queries: ' + str(n_consecutive_dup))
    for query in li:
        count += 1
        f_popular_queries.write( '\n' + query[0] + '\t' + str(query[1]) + '\t' + str(float(query[1])/n_queries) )
        if count > MAX_OUTPUT:
            break

    # print term related statistics
    li_term = sorted(term_dic.iteritems(), key=lambda d:d[1], reverse = True)
    count = 0
    f_stat.write('\noverall terms                 : ' + str(n_terms))
    f_stat.write('\nunique terms                  : ' + str(len(term_dic)))
    li_term_stat = sorted(term_stat_dic.iteritems(), key=lambda d:int(d[0]), reverse = False)
    f_stat.write('\nnumnber of terms per query: (#terms, #queries, #ratio)')
    for term_stat in li_term_stat:
        f_stat.write( '\n(' + term_stat[0] + '\t' +str(term_stat[1]) + '\t' + str(float(term_stat[1])/n_queries) + ')' )
        
    for term in li_term:
        count += 1
        f_popular_terms.write( '\n' + term[0] + '\t' + str(term[1]) + '\t' + str(float(term[1])/n_terms) )
        if count > MAX_OUTPUT:
            break

    f_stat.close()
    f_popular_queries.close()
    f_popular_terms.close()

def correlation_analyze(input_file, output_dir):
    # term co-occurance
    # query
    print ('get in correlation_analyze')

if __name__=='__main__':
    # print help
    if len(sys.argv)!=2:
        print('Usage: python analyze_query_log.py [input_name]  (results stored in input_name_analysis)')
        exit(1)
    
    # do analysis
    input_file = open(sys.argv[1])
    output_dir = sys.argv[1] + '_analysis'

    call(['rm', '-r', output_dir])
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    os.chdir(output_dir)
    call(['pwd'])

    # first: counting job
    counting_analyze(input_file, output_dir)
    # second: correlation analysis 
    correlation_analyze(input_file, output_dir)

    input_file.close()
