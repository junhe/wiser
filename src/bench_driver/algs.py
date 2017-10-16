"""
This file contains algorithm contents as strings. It is
easier to manage this way than to put them in separate files.
"""





def CREATE_LINE_DOC(line_file_out, docs_file):
    text = """
content.source=org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource
line.file.out=%(out)s
docs.file=%(docs)s
keep.image.only.docs = false
content.source.forever=false

# -------------------------------------------------------------------------------------

{WriteLineDoc()}: *
""" % {'out':line_file_out, 'docs':docs_file}
    print text
    return text


def INDEX_LINE_DOC(docs_file, work_dir, index_doc_count):
    text = """
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

docs.file=%(docs)s
work.dir=%(work_dir)s

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
        { "MAddDocs" AddDoc } : %(index_doc_count)s
        # ForceMerge(1)
        CloseIndex
    }


    RepSumByPref MAddDocs

    NewRound

} : 1

RepSumByNameRound
RepSumByName
RepSumByPrefRound MAddDocs
""" % {'docs': docs_file, 'work_dir': work_dir, 'index_doc_count': index_doc_count}
    return text



def REUTER_SEARCH(docs_file, work_dir, search_count):
    text = """
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

work.dir=%(work_dir)s
docs.file=%(docs)s

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

    OpenReader
    { "SearchSameRdr" Search > : %(search_count)s
    CloseReader

    RepSumByPref MAddDocs

} : 1

RepSumByNameRound
RepSumByName
RepSumByPrefRound MAddDocs
"""% {'docs': docs_file, 'work_dir': work_dir, 'search_count': search_count}

    return text



