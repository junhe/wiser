"""
This file contains algorithm contents as strings. It is
easier to manage this way than to put them in separate files.
"""




CREATE_LINE_DOC = """
content.source=org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource
line.file.out=work/enwiki.out.txt
docs.file=temp/enwiki-latest-pages-articles9.xml-p1791081p2336422
keep.image.only.docs = false
content.source.forever=false

# -------------------------------------------------------------------------------------

{WriteLineDoc()}: *
"""





INDEX_LINE_DOC = """
writer.version=4.0
merge.factor=mrg:10:100:10:100:10:100:10:100
max.buffered=buf:10:10:100:100:10:10:100:100
#ram.flush.mb=flush:32:40:48:56:32:40:48:56
compound=cmpnd:true:true:true:true:false:false:false:false

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory
#directory=RamDirectory

doc.stored=true
doc.tokenized=true
doc.term.vector=false
log.step=2000

#docs.dir=reuters-out
#docs.dir=reuters-111

docs.file=work/enwiki.out.txt

#content.source=org.apache.lucene.benchmark.byTask.feeds.SingleDocSource
#content.source=org.apache.lucene.benchmark.byTask.feeds.ReutersContentSource
content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource

#query.maker=org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker
query.maker=org.apache.lucene.benchmark.byTask.feeds.ReutersQueryMaker

# task at this depth or less would print when they start
task.max.depth.log=2

log.queries=true
# -------------------------------------------------------------------------------------

{ "Rounds"

    ResetSystemErase

    { "Populate"
        CreateIndex
        { "MAddDocs" AddDoc } : 20000
        ForceMerge(1)
        CloseIndex
    }


    RepSumByPref MAddDocs

    NewRound

} : 1

RepSumByNameRound
RepSumByName
RepSumByPrefRound MAddDocs
"""


