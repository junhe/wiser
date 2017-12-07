import sys
import requests, json
import codecs


parse_flag = False
#parse_flag = True
doc_num = 1
headers = {'Content-Type': 'application/json'}
url_setting = 'http://localhost:9200/my_index?pretty'
url_index = 'http://localhost:9200/my_index/my_type/1?pretty'
url_analyze = 'http://localhost:9200/my_index/_analyze?pretty'
url_search = 'http://localhost:9200/my_index/_search?pretty'

def add_document():
    ## delete old index
    r = requests.delete(url_setting)

    ## setting analyzer
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
      },
      "mappings": {
        "my_type": {
           "properties": {
              "body": {
                 "type":"text",
                 "index_options" : "offsets",
                 "analyzer": "my_english_analyzer",
                 "search_analyzer":"my_english_analyzer"
              }
           }
         }
       }
    }
    """
    r = requests.put(url_setting, payload, headers=headers)
    print(r.text)

    ######## Index a Document
    
    input_file = open('line_doc_offset')
    header = input_file.readline()
    for line in input_file:
        items = line.split("\t")
        doc = {}
        doc_content = items[1]
        doc["body"] = doc_content
        doc_result = json.dumps(doc)
        print "====", doc_result
        r = requests.put(url_index, doc_result, headers=headers)
        print(r.text)

        ####### Analyze this document
        '''
        doc = {}
        doc["field"] = "body"
        doc["text"] = doc_content
        doc_result = json.dumps(doc)
        r = requests.post(url_analyze, doc_result, headers=headers)
        print(r.text)
        '''

def benchmark():
    ######## Query on
    query_payload = """
    {
      "query": {
        "terms": {
          "body" : ["rule"]
        }
      },
      "highlight" : {
        "number_of_fragments" : 10,
        "fragment_size" : 0,
        "fields" : {
            "body" : {}
        }
      }
    }
    """

    print "====", query_payload
    r = requests.post(url_search, query_payload, headers=headers)
    print(r.text)

if __name__=='__main__':
    # print help
    if len(sys.argv)!=1:
        print('Usage: python elasticsearch_highlighter_bench.py')
        exit(1)
    
    # do benchmark
    if parse_flag:
        add_document()
    else:
        benchmark()
