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

    QueryParser qp = new QueryParser(DocMaker.BODY_FIELD, anlzr);
    ArrayList<Query> qq = new ArrayList<>();

    try {
        String path = config.get("wiki.query.log.path", "");
        System.out.println("Query Log: " + path);

        int cnt = config.get("wiki.query.count", 10000);
        System.out.println("Query Count: " + cnt);

        File file = new File(path);
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        StringBuffer stringBuffer = new StringBuffer();
        String line;
        while ((line = bufferedReader.readLine()) != null && cnt != 0) {
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
