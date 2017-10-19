#/**
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# -------------------------------------------------------------------------------------

#
# This alg will process the Reuters documents feed to produce a
# single file that contains all documents, one per line.
#
# To use this, first cd to benchmark and then run:
#
#   ant run-task -Dtask.alg=conf/createLineFile.alg
#
# Then, to index the documents in the line file, see
# indexLineFile.alg.
#
#
# It does not speicify the doc maker, so it should use the default one.

# Where to get documents from:
content.source=org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource

# Where to write the line file output:
line.file.out=work/enwiki.out.txt

docs.file=temp/enwiki-latest-pages-articles9.xml-p1791081p2336422

keep.image.only.docs = false

# Stop after processing the document feed once:
content.source.forever=false

# -------------------------------------------------------------------------------------

# Process all documents, appending each one to the line file:
{WriteLineDoc()}: * 
