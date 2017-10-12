from pyreuse.helpers import cd, shcmd

CP = ":".join(["build/core/lucene-core-7.0.1-SNAPSHOT.jar",
        "build/queryparser/lucene-queryparser-7.0.1-SNAPSHOT.jar",
        "build/analysis/common/lucene-analyzers-common-7.0.1-SNAPSHOT.jar",
        "build/demo/lucene-demo-7.0.1-SNAPSHOT.jar"])

with cd("../../lucene-7.0.1"):
    shcmd("ant compile") # ant dist?
    shcmd("java -cp {} org.apache.lucene.demo.IndexFiles -docs .".format(CP))
    shcmd("java -cp {} org.apache.lucene.demo.SearchFiles ".format(CP))

