import sys
import requests, json
import codecs
import re
import io
import unicodedata

def tokenize(query_log, output):

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
    
    i = 0
    j = 0
    with requests.Session() as session:

        for line in query_log:
            i+=1
            if i % 100000 == 0:
                print 'Finished ', i, 'documents'
            items = line.replace('AND', ' ').replace('\"', '')
            try:
                items.decode('utf-8')
            except UnicodeError:
                continue

            #print items
            try: 
                doc_content = unicodedata.normalize('NFKD', unicode(items)).encode('ascii', 'ignore')
            except:
                j += 1
                continue

            doc["text"] = doc_content
            doc_result = json.dumps(doc)
            #r = requests.post(url, doc_result, headers=headers)
            r = session.post(url, doc_result, headers=headers)
            query_res = ''
            for token in r.json()["tokens"]:
                query_res += unicodedata.normalize('NFKD', unicode(token['token'])).encode('ascii', 'ignore') + ' '
            if query_res != '':
                output.write( unicode(query_res + '\n') )
    print '=== overall get : ', j, 'execptions'

if __name__=='__main__':
    # print help
    if len(sys.argv)!=2:
        print('Usage: python tokenize_querylog.py [input_name]  (results stored in input_name_tokenized)')
        exit(1)
    
    # do analysis
    #line_doc = open(sys.argv[1])
    output = io.open(sys.argv[1] + '_tokenized', 'w', encoding="utf-8")
    with open(sys.argv[1], "r") as line_doc:
        tokenize(line_doc, output)
    #line_doc.close()
    output.close()
