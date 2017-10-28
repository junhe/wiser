import sys
import requests, json
import codecs

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
    output.write(header)
    for line in line_doc:
        items = line.split("\t")
        doc_content = items[2]
        doc["text"] = doc_content
        doc_result = json.dumps(doc)
        r = requests.post(url, doc_result, headers=headers)
        #print(r.text)
        output.write(items[0]+'\t' + items[1]+'\t')
        
        for token in r.json()["tokens"]:
            output.write(token["token"].encode('utf8') + ' ')
        
        output.write('\n')

if __name__=='__main__':
    # print help
    if len(sys.argv)!=2:
        print('Usage: python tokenize_wiki_linedoc.py [input_name]  (results stored in input_name_tokenized)')
        exit(1)
    
    # do analysis
    line_doc = open(sys.argv[1])
    output = open(sys.argv[1] + '_tokenized', 'w')
    
    tokenize(line_doc, output)
    line_doc.close()
    output.close()
