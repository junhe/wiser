/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.benchmark.byTask.feeds;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.benchmark.byTask.tasks.NewAnalyzerTask;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


import java.util.ArrayList;

/**
 * A QueryMaker that makes queries for a collection created 
 * using {@link org.apache.lucene.benchmark.byTask.feeds.SingleDocSource}.
 */
public class WikiQueryMaker extends AbstractQueryMaker implements QueryMaker {


  /**
   * Prepare the queries for this test.
   * Extending classes can override this method for preparing different queries. 
   * @return prepared queries.
   * @throws Exception if cannot prepare the queries.
   */
  @Override
  protected Query[] prepareQueries() throws Exception {
    // analyzer (default is standard analyzer)
    Analyzer anlzr= NewAnalyzerTask.createAnalyzer(config.get("analyzer",
        "org.apache.lucene.analysis.standard.StandardAnalyzer")); 

    QueryParser qp = new QueryParser(DocMaker.BODY_FIELD,anlzr);
    ArrayList<Query> qq = new ArrayList<>();
    Query q1 = new TermQuery(new Term(DocMaker.ID_FIELD,"doc2"));
    //qq.add(q1);
    Query q2 = new TermQuery(new Term(DocMaker.BODY_FIELD,"simple"));
    //qq.add(q2);
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    //bq.add(q1,Occur.MUST);
    //bq.add(q2,Occur.MUST);
    //qq.add(bq.build());
    //qq.add(qp.parse("\"open\" AND \"interest\""));
    //qq.add(qp.parse("synthetic body"));
    //qq.add(qp.parse("\"synthetic body\""));
    //qq.add(qp.parse("synthetic text"));
    //qq.add(qp.parse("\"synthetic text\""));
    //qq.add(qp.parse("\"synthetic text\"~3"));
    //qq.add(qp.parse("zoom*"));
    //qq.add(qp.parse("synth*"));

    try {
        //File file = new File("/mnt/ssd/text.txt");
        File file = new File("/mnt/ssd/downloads/wiki_QueryLog");
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        StringBuffer stringBuffer = new StringBuffer();
        String line;
        int cnt = 100000;
        while ((line = bufferedReader.readLine()) != null && cnt != 0) {
            if (line.contains("&"))
                continue;
            qq.add(qp.parse(line));
            cnt--;
        }
        fileReader.close();
    } catch (IOException e) {
        e.printStackTrace();
    }

    return  qq.toArray(new Query[0]);
  }

}
