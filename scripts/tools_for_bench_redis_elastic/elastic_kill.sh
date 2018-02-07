#curl -XDELETE 'localhost:9200/wik?pretty'
kill -9 `ps -aef | grep 'java' | grep -v grep | awk '{print $2}'`
