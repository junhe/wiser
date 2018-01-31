# lucene src files must be in place

cp ./conf/wikipedia2.alg ../../lucene-7.0.1/benchmark/conf/

cd ../../lucene-7.0.1/benchmark/

mkdir -p temp
cd temp
#wget -nc https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles9.xml-p1791081p2336422.bz2
#bunzip2 enwiki-latest-pages-articles9.xml-p1791081p2336422.bz2
ln -s /mnt/ssd/enwiki-latest-pages-articles.xml ./enwiki-latest-pages-articles9.xml-p1791081p2336422


cd ../
pwd
ant run-task -Dtask.alg=conf/wikipedia2.alg


