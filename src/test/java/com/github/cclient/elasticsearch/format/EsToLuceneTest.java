package com.github.cclient.elasticsearch.format;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.*;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;


public class EsToLuceneTest {
    EsToLucene esToLucene;

    @Before
    public void init() {
        try {
            esToLucene = new EsToLucene();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void formatQueryStringToLucenePhrase() {
        String elasticsearchQueryString = "((\"hello\" OR \"world\") NOT (\"hi\" OR \"china\"))";
        String lucenePharse;
        try {
            lucenePharse = esToLucene.formatQueryStringToLucenePhrase(elasticsearchQueryString, new StandardAnalyzer());
            System.out.println("lucenePharse: " + lucenePharse);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void parseQueryStringToLuceneQuery() {
        String elasticsearchQueryString = "((\"hello\" OR \"world\") NOT (\"hi\" OR \"china\"))";
        Query query;
        try {
            query = esToLucene.parseQueryStringToLuceneQuery(elasticsearchQueryString, Arrays.asList("content"), new StandardAnalyzer());
            System.out.println("luceneQuery: " + query);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}