package com.github.cclient.elasticsearch.format;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.xml.builders.RangeQueryBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class EsToLuceneSearchTest {

    Directory directory;

    @Test
    public void createRAMIndex() {
        HashMap<String, Object> doc0 = new HashMap<String, Object>();
        doc0.put("_id", "0");
        doc0.put("long_view", 300);
        doc0.put("text_body", "Learn Git and GitHub without any code!");
        HashMap<String, Object> doc1 = new HashMap<String, Object>();
        doc1.put("_id", "1");
        doc1.put("long_view", 100);
        doc1.put("text_body", "Using the Hello World guide, you’ll create a repository, start a branch, write comments, and open a pull request");
        HashMap<String, Object> doc2 = new HashMap<String, Object>();
        doc2.put("_id", "2");
        doc2.put("long_view", 222);
        doc2.put("text_body", "Here are some quick tips for a first-time organization member.");
        HashMap<String, Object> doc3 = new HashMap<String, Object>();
        doc3.put("_id", "3");
        doc3.put("long_view", 23);
        doc3.put("text_body", "After you switch contexts you’ll see an organization-focused dashboard that lists out organization repositories and activities");
        try {
            directory = EsToLuceneSearch.createRAMIndex(Arrays.asList(doc0, doc1, doc2, doc3), new StandardAnalyzer(), Arrays.asList("long_view", "text_body"));
            System.out.println("directory: " + directory);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void filter() {
        createRAMIndex();
        System.out.println("directory: " + directory);
        try {
            EsToLucene esToLucene = new EsToLucene();
            String elasticsearchQueryString = "(Git code guide) OR (repository branch)";
            Query textQuery = esToLucene.parseQueryStringToLuceneQuery(elasticsearchQueryString, Arrays.asList("content"), new StandardAnalyzer());
            Query rangeQuery=TermRangeQuery.newStringRange("long_view", "0", "1000", true, true);
            BooleanQuery.BuildWer booleanQuery = new BooleanQuery.Builder();
            booleanQuery.add(rangeQuery, BooleanClause.Occur.SHOULD);
            booleanQuery.add(textQuery, BooleanClause.Occur.SHOULD);
            Query compileQuery = booleanQuery.build();

            System.out.println("query: " + compileQuery.toString());
            List<String> matchIds = EsToLuceneSearch.filter(directory, compileQuery, 100);
            System.out.println("matchIds: " + matchIds);

    } catch (Exception e) {
            e.printStackTrace();
        }
    }
}