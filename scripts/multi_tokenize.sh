#!/bin/bash

for f in /mnt/ssd/wiki_querylog/files/*.txt;
do
  echo "Processing $f file..."
  python /users/kanwu/flashsearch/scripts/tokenize_querylog.py $f
  rm $f
  
  # take action on each file. $f store current file name
  #cat $f
done

