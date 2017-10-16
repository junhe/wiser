import re
import os
import sys 

def counting_analyze(input_file, output_file):
    text = input_file.readlines()
    # term frequency                     done
    # query frequency                    done
    # exact same-as-before queries       done
    # number of terms in queries
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
    print ('overall queries               : ' + str(n_queries))
    print ('unique queries                : ' + str(len(query_dic)))
    print ('exactly-same-as-before queries: ' + str(n_consecutive_dup))
    for query in li:
        count += 1
        print ( query[0] + '\t' + str(query[1]) + '\t' + str(float(query[1])/n_queries) )
        if count > 100:
            break

    # print term related statistics
    li_term = sorted(term_dic.iteritems(), key=lambda d:d[1], reverse = True)
    count = 0
    print ('overall terms                 : ' + str(n_terms))
    print ('unique terms                  : ' + str(len(term_dic)))
    li_term_stat = sorted(term_stat_dic.iteritems(), key=lambda d:d[1], reverse = True)
    for term_stat in li_term_stat:
        print ( '(' + term_stat[0] + '\t' +str(term_stat[1]) + '\t' + str(float(term_stat[1])/n_queries) + ')' )
        
    for term in li_term:
        count += 1
        print ( term[0] + '\t' + str(term[1]) + '\t' + str(float(term[1])/n_terms) )
        if count > 100:
            break

def correlation_analyze(input_file, output_file):
    # term co-occurance
    # query
    print ('get in correlation_analyze')

if __name__=='__main__':
    # print help
    if len(sys.argv)!=3:
        print('Usage: python analyze_query_log.py [input_name] [output_name]')
        exit(1)
    
    # do analysis
    input_file = open(sys.argv[1])
    output_file = open(sys.argv[2],'w')


    # first: counting job
    counting_analyze(input_file, output_file)
    # second: correlation analysis 
    correlation_analyze(input_file, output_file)

    input_file.close()
    output_file.close()     
