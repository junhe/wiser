
import requests, json

url_setting = 'http://localhost:9200/my_index?pretty'
url = 'http://localhost:9200/my_index/_analyze?pretty'
headers = {'Content-Type': 'application/json'}

payload = """
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_english_analyzer": {
          "type": "standard",
          "stopwords": "_english_"
        }
      }
    }
  }
}
"""
#r = requests.put(url_setting, payload, headers=headers)
#print r.text

data = """
{
  "analyzer": "my_english_analyzer",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog\u0027s bone."
}
"""
r = requests.post(url, data, headers=headers)
print r.text

