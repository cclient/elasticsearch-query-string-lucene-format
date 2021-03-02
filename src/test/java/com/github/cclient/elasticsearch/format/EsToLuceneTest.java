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

    public void rangeQuery() throws Exception {
        AbstractBuilderTestCase c = new AbstractBuilderTestCase();
        c.beforeTest();
        QueryShardContext qc = AbstractBuilderTestCase.createShardContext();
        //实例构造
        RangeQueryBuilder rqb = new RangeQueryBuilder("rangeFieldName");
        rqb.from(1);
        rqb.to(100);
        Query q = rqb.toQuery(qc);
        System.out.println("to Query : " + q);
        Query f = rqb.toFilter(qc);
        System.out.println("to Filter: " + f);
        String query =
                "{\n" +
                        "    \"range\":{\n" +
                        "        \"rangeFieldName\": {\n" +
                        "            \"gt\": 1\n" +
                        "        }\n" +
                        "    }\n" +
                        "}";
//        XContentParser parser =
//                JsonXContent.jsonXContent.createParser(
//                        new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
//                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
//                        rangeAggregation);
        //报错ParsingException[[range] query does not support [range]
//        RangeQueryBuilder rqbFromJson=RangeQueryBuilder.fromXContent(parser);
        //        报错 ParsingException[no [query] registered for [range]
//        org.elasticsearch.index.query.QueryBuilder parseInnerQueryBuilder = RangeQueryBuilder.parseInnerQueryBuilder(parser);
        EsToLuceneQueryParser EsToLuceneQueryParser = new EsToLuceneQueryParser(qc.getXContentRegistry());
        org.elasticsearch.index.query.QueryBuilder queryBuilder = EsToLuceneQueryParser.parseQuery(query);
        Query rqbFromJson = queryBuilder.toQuery(qc);
        System.out.println("to Query : " + rqbFromJson);
    }


    @Test
    public void textAndRangeQuery() throws Exception {
        AbstractBuilderTestCase c = new AbstractBuilderTestCase();
        c.beforeTest();
        QueryShardContext qc = AbstractBuilderTestCase.createShardContext();
        String query =
                "{\n" +
                        "    \"range\":{\n" +
                        "        \"rangeFieldName\": {\n" +
                        "            \"gt\": 1\n" +
                        "        }\n" +
                        "    }\n" +
                        "}";
        EsToLuceneQueryParser EsToLuceneQueryParser = new EsToLuceneQueryParser(qc.getXContentRegistry());
        org.elasticsearch.index.query.QueryBuilder queryBuilder = EsToLuceneQueryParser.parseQuery(query);
        Query rqbFromJson = queryBuilder.toQuery(qc);
        Query textQuery = esToLucene.parseQueryStringToLuceneQuery("((\"hello\" OR \"word\") NOT (\"hi\" OR \"github\"))", Arrays.asList("textFieldName"), new StandardAnalyzer());
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        booleanQuery.add(rqbFromJson, BooleanClause.Occur.SHOULD);
        booleanQuery.add(textQuery, BooleanClause.Occur.SHOULD);
        Query compileQuery = booleanQuery.build();
        System.out.println("to Query : " + compileQuery);
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

    @Test
    public void parseNoAnalyzerToLuceneQuery() {
        String rangeQureyJson = "{\n" +
                "         \"range\": {\n" +
                "             \"date\": {\n" +
                "                 \"gte\": \"2021-02-03T00:00:00\",\n" +
                "                 \"lte\": \"2021-02-04T00:00:00\"\n" +
                "             }\n" +
                "         }\n" +
                "     }";
        String termQueryJson = "{\n" +
                "    \"term\": {\n" +
                "        \"www.github.com\": false\n" +
                "    }\n" +
                "}";
        String termsQueryJson = "{\n" +
                "    \"terms\": {\n" +
                "        \"domain\": [\n" +
                "            \"www.github.com\",\n" +
                "            \"cclient\"\n" +
                "        ]\n" +
                "    }\n" +
                "}";
        //todo not work
        String existsQueryJson = "{\n" +
                "    \"exists\": {\n" +
                "        \"field\": \"title\"\n" +
                "    }\n" +
                "}";
        String nestedExistsQueryJson = "{\n" +
                "    \"nested\": {\n" +
                "        \"path\": \"nestedTitle\",\n" +
                "        \"ignore_unmapped\": true,\n" +
                "        \"query\": {\n" +
                "            \"exists\": {\n" +
                "                \"field\": \"nestedTitle.date\"\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        String nestedRangeQueryJson = "{\n" +
                "    \"nested\": {\n" +
                "        \"path\": \"nestedTitle\",\n" +
                "        \"ignore_unmapped\": true,\n" +
                "        \"query\": {\n" +
                "            \"range\": {\n" +
                "                \"date\": {\n" +
                "                    \"gt\": \"2021-01-19T00:00:00\"\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        List<String> queryJsons = Arrays.asList(rangeQureyJson, termQueryJson, termsQueryJson, existsQueryJson, nestedExistsQueryJson, nestedRangeQueryJson);
        queryJsons.forEach(json -> {
            try {
                System.out.println("json: " + json);
                Query query = esToLucene.parseNoAnalyzerToLuceneQuery(json);
                System.out.println("query: " + query);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }

    @Test
    public void parseQueryToLuceneQuery() {
        String withOutAnalyzer = "{\n" +
                "    \"query\": {\n" +
                "        \"bool\": {\n" +
                "            \"must\": [\n" +
                "                {\n" +
                "                    \"terms\": {\n" +
                "                        \"domain\": [\n" +
                "                            \"www.github.com\"\n" +
                "                        ]\n" +
                "                    }\n" +
                "                },\n" +
                "                {\n" +
                "                    \"range\": {\n" +
                "                        \"date\": {\n" +
                "                            \"gte\": \"2021-02-03T00:00:00\",\n" +
                "                            \"lte\": \"2021-02-04T00:00:00\"\n" +
                "                        }\n" +
                "                    }\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";

        String withNoExistsAnalyzer = "{\n" +
                "    \"query\": {\n" +
                "        \"bool\": {\n" +
                "            \"must\": [\n" +
                "                {\n" +
                "                    \"query_string\": {\n" +
                "                        \"analyzer\": \"ik_smart\",\n" +
                "                        \"query\": \"hello word\",\n" +
                "                        \"fields\": [\n" +
                "                            \"content\",\n" +
                "                            \"title\"\n" +
                "                        ]\n" +
                "                    }\n" +
                "                },\n" +
                "                {\n" +
                "                    \"terms\": {\n" +
                "                        \"domain\": [\n" +
                "                            \"www.github.com\"\n" +
                "                        ]\n" +
                "                    }\n" +
                "                },\n" +
                "                {\n" +
                "                    \"range\": {\n" +
                "                        \"date\": {\n" +
                "                            \"gte\": \"2021-02-03T00:00:00\",\n" +
                "                            \"lte\": \"2021-02-04T00:00:00\"\n" +
                "                        }\n" +
                "                    }\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";

        String withExistsAnalyzer = "{\n" +
                "    \"query\": {\n" +
                "        \"bool\": {\n" +
                "            \"must\": [\n" +
                "                {\n" +
                "                    \"query_string\": {\n" +
                "                        \"analyzer\": \"standard\",\n" +
                "                        \"query\": \"hello word\",\n" +
                "                        \"fields\": [\n" +
                "                            \"content\",\n" +
                "                            \"title\"\n" +
                "                        ]\n" +
                "                    }\n" +
                "                },\n" +
                "                {\n" +
                "                    \"terms\": {\n" +
                "                        \"domain\": [\n" +
                "                            \"www.github.com\"\n" +
                "                        ]\n" +
                "                    }\n" +
                "                },\n" +
                "                {\n" +
                "                    \"range\": {\n" +
                "                        \"date\": {\n" +
                "                            \"gte\": \"2021-02-03T00:00:00\",\n" +
                "                            \"lte\": \"2021-02-04T00:00:00\"\n" +
                "                        }\n" +
                "                    }\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";

        List<String> queryJsons = Arrays.asList(withExistsAnalyzer, withOutAnalyzer, withNoExistsAnalyzer);
        queryJsons.forEach(json -> {
            try {
                System.out.println("json: " + json);
                Query query = esToLucene.parseQueryToLuceneQuery(json, Arrays.asList("content", "title"), new StandardAnalyzer());
                System.out.println("query: " + query);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}