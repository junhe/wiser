import sys
import requests, json
import codecs
import re

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
    #output.write(header.strip('\n') + "\t" + "tokenized\n")
    for line in line_doc:
        items = line.split("\t")
        doc_content = items[1]
        doc["text"] = doc_content
        doc_result = json.dumps(doc)
        r = requests.post(url, doc_result, headers=headers)
        output.write(items[0]+'\t' + items[1].strip('\n')+'\t')
        
        # unique and collect offsets
        dic = {}
        for token in r.json()["tokens"]:
            # also store offsets

            if token["token"].encode('utf8') in dic:
                tmp = dic[token["token"].encode('utf8')] 
                tmp.append((token["start_offset"], token["end_offset"]))
                dic[token["token"].encode('utf8')] = tmp
                continue
            dic[token["token"].encode('utf8')] = [(token["start_offset"], token["end_offset"])]

        terms = ""
        offsets = ""
        for token in dic:
            terms += token + ' '
            offsets += str(dic[token]) + '.'
            print token, ': ', dic[token]
        offsets = offsets.replace()
        #output.write(token["token"].encode('utf8') + ' ')
        output.write( '\n' + terms + '\t' + offsets)
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
