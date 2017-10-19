wget http://www.cim.mcgill.ca/~dudek/206/Logs/AOL-user-ct-collection/aol-data.tar.gz
tar -xzvf ./aol-data.tar.gz
cd ./AOL-user-ct-collection/
gunzip -d ./*.gz
rm ./U500k_README.txt
