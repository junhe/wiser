import sys
import requests, json
import codecs
import re
import io
import unicodedata

def tokenize(line_doc, output):

    # setting url
    url_setting = 'http://localhost:9200/my_index?pretty'
    headers = {'Content-Type': 'application/json'}

    ## delete old index
    r = requests.delete(url_setting)
    #print r.text

    ## setting analyzer
	      #"tokenizer":  "icu_tokenizer",
      	      #"char_filter": ["icu_normalizer"],
    payload = """
    {
      "settings": {
        "analysis": {
          "analyzer": {
            "my_english_analyzer": {
	      "tokenizer":  "standard",
	      "char_filter":  [ "html_strip" ],
	      "filter" : ["english_possessive_stemmer", 
			"lowercase", "english_stop", 
			"english_stemmer", 
			"asciifolding", "icu_folding"]
            }
          },
          "filter" : {
		"english_stop": {
          		"type":       "stop",
          		"stopwords":  "_english_"
        	},
                "english_possessive_stemmer": {
          	    "type":       "stemmer",
          	    "language":   "possessive_english" 
        	},
		"english_stemmer" : {
                    "type" : "stemmer",
                    "name" : "english"
                },
		"my_folding": {
        		"type": "asciifolding",
        		"preserve_original": "false"
      		}
          }
        }
      }  
  
    }
    """
    r = requests.put(url_setting, payload, headers=headers)
    print(r.text)


    # parse each file
    url = 'http://localhost:9200/my_index/_analyze?pretty'
    doc = {}
    doc["analyzer"] = "my_english_analyzer"
    
    header = line_doc.readline()
    output.write(u"FIELDS_HEADER_INDICATOR###\tdoctitle\tbody\ttokenized\toffsets\tpositions\n")
    i = 0
    for line in line_doc:
        i+=1
        if i % 10000 == 0:
            print 'Finished ', i, 'documents'
        items = line.split("\t")
        #doc_content = items[1]
        doc_content = unicodedata.normalize('NFKD', unicode(items[1])).encode('ascii', 'ignore')
        doc_content = doc_content.strip('\n').replace('&', '').replace('<','').replace('>','')#.replace('\"','').replace('\'','')
        doc_title = unicodedata.normalize('NFKD', unicode(items[0])).encode('ascii', 'ignore')
        doc["text"] = doc_content
        doc_result = json.dumps(doc)
        r = requests.post(url, doc_result, headers=headers)
        #print r.text
        #output.write(unicode(items[0] +'\t' + items[1].strip('\n') +'\t'))
        output.write(unicode(doc_title +'\t' + doc_content.strip('\n') +'\t')) 
        # unique and collect offsets
        dic = {}
        dic_pos = {}
        for token in r.json()["tokens"]:
            # also store offsets

            #if token["token"].encode('utf8') in dic:
            token_str = unicodedata.normalize('NFKD', unicode(token['token'])).encode('ascii', 'ignore')
            if token_str in dic:
                # for offsets
                tmp = dic[token_str]
                tmp += (str(token["start_offset"]) + ',' + str(token["end_offset"]) + ';')
                dic[token_str] = tmp
                # for positions
                tmp = dic_pos[token_str]
                tmp += ( str(token["position"])+ ';')
                dic_pos[token_str] = tmp

                continue
            dic[token_str] = str(token["start_offset"]) + ',' + str(token["end_offset"]) + ';'
            dic_pos[token_str] = str(token["position"]) + ';'

        terms = ""
        offsets = ""
        positions = ""
        for token in dic:
            terms += token + ' '
            offsets += dic[token] + '.'
            positions += dic_pos[token] + '.'
            #print token, ': ', dic[token]
        output.write( unicode(terms + '\t' + offsets + '\t' + positions) )
        output.write( unicode('\n') )

if __name__=='__main__':
    # print help
    if len(sys.argv)!=2:
        print('Usage: python tokenize_wiki_linedoc.py [input_name]  (results stored in input_name_tokenized)')
        exit(1)
    
    # do analysis
    #line_doc = open(sys.argv[1])
    output = io.open(sys.argv[1] + '_tokenized', 'w', encoding="utf-8")
    with io.open(sys.argv[1], "r", encoding="utf-8") as line_doc:
        tokenize(line_doc, output)
    #line_doc.close()
    output.close()

