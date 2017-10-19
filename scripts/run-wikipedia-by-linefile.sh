# create line file from wikipedia
# index the line file

cp ./conf/create-line-file.alg ../../lucene-7.0.1/benchmark/conf/
cp ./conf/index-line-file-2.alg ../../lucene-7.0.1/benchmark/conf/

cd ../../lucene-7.0.1/benchmark/

mkdir -p temp
cd temp
wget -nc https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles9.xml-p1791081p2336422.bz2
bunzip2 enwiki-latest-pages-articles9.xml-p1791081p2336422.bz2

cd ../
pwd
ant run-task -Dtask.alg=conf/create-line-file.alg
ant run-task -Dtask.alg=conf/index-line-file-2.alg


