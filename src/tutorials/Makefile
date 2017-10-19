#CORE=~/lib/lucene-7.0.1/core/lucene-core-7.0.1.jar
#QUERYPARSER=~/lib/lucene-7.0.1/queryparser/lucene-queryparser-7.0.1.jar
#ANALYZERCOMMON=~/lib/lucene-7.0.1/analysis/common/lucene-analyzers-common-7.0.1.jar
#DEMO=~/lib/lucene-7.0.1/demo/lucene-demo-7.0.1.jar

CORE=../lucene/build/core/lucene-core-7.0.1-SNAPSHOT.jar
QUERYPARSER=../lucene/build/queryparser/lucene-queryparser-7.0.1-SNAPSHOT.jar
ANALYZERCOMMON=../lucene/build/analysis/common/lucene-analyzers-common-7.0.1-SNAPSHOT.jar

helloworld:
	javac HelloWorld.java
	java HelloWorld

indexandsearch:
	#javac -cp ~/lib/lucene-7.0.1/core/lucene-core-7.0.1.jar:~/lib/lucene-7.0.1/queryparser/lucene-queryparser-7.0.1.jar:~/lib/lucene-7.0.1/analysis/common/lucene-analyzers-common-7.0.1.jar:~/lib/lucene-7.0.1/demo/lucene-demo-7.0.1.jar IndexAndSearch.java
	#javap -cp ~/lib/lucene-7.0.1/core/lucene-core-7.0.1.jar:~/lib/lucene-7.0.1/queryparser/lucene-queryparser-7.0.1.jar:~/lib/lucene-7.0.1/analysis/common/lucene-analyzers-common-7.0.1.jar:~/lib/lucene-7.0.1/demo/lucene-demo-7.0.1.jar  IndexAndSearch
	#javap -cp ~/lib/lucene-7.0.1/core/lucene-core-7.0.1.jar:~/lib/lucene-7.0.1/queryparser/lucene-queryparser-7.0.1.jar:~/lib/lucene-7.0.1/analysis/common/lucene-analyzers-common-7.0.1.jar:~/lib/lucene-7.0.1/demo/lucene-demo-7.0.1.jar  IndexAndSearch
	#java -cp .:${CORE}  IndexAndSearch
	#java -cp ${CORE}:./IndexAndSearch.class  IndexAndSearch
	#java -cp .:${CORE}:${ANALYZERCOMMON}:${QUERYPARSER}:${DEMO} IndexAndSearch
	javac -cp .:${CORE}:${ANALYZERCOMMON}:${QUERYPARSER} IndexAndSearch.java
	java -cp .:${CORE}:${ANALYZERCOMMON}:${QUERYPARSER} IndexAndSearch

indexfiles:
	#javac -cp ~/lib/lucene-7.0.1/core/lucene-core-7.0.1.jar:~/lib/lucene-7.0.1/queryparser/lucene-queryparser-7.0.1.jar:~/lib/lucene-7.0.1/analysis/common/lucene-analyzers-common-7.0.1.jar:~/lib/lucene-7.0.1/demo/lucene-demo-7.0.1.jar IndexFiles.java
	javac -cp ~/lib/lucene-7.0.1/core/lucene-core-7.0.1.jar IndexFiles.java
	#javac -cp ~/lib/lucene-7.0.1/analysis/common/lucene-analyzers-common-7.0.1.jar IndexFiles.java
	#java -cp ~/lib/lucene-7.0.1/core/lucene-core-7.0.1.jar jun.IndexFiles
	java jun.IndexFiles


hellolucene:
	javac -cp .:${CORE}:${ANALYZERCOMMON}:${QUERYPARSER} HelloLucene.java
	java -cp .:${CORE}:${ANALYZERCOMMON}:${QUERYPARSER} HelloLucene

