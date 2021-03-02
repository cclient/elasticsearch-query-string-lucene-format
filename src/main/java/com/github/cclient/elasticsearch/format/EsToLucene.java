package com.github.cclient.elasticsearch.format;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.search.QueryStringQueryParser;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;

import java.util.Arrays;
import java.util.List;

public class EsToLucene {
    static String fileNameMask = "6f431aa9a06591e7f45239a0caa82c9f";
    static List<String> fileNameMasks = Arrays.asList(fileNameMask);
    QueryShardContext queryShardContext;
    EsToLuceneSearchParser esToLuceneSearchParser;

    public EsToLucene() throws Exception {
        AbstractBuilderTestCase c = new AbstractBuilderTestCase();
        c.beforeTest();
        queryShardContext = AbstractBuilderTestCase.createShardContext();
        esToLuceneSearchParser = new EsToLuceneSearchParser();
        esToLuceneSearchParser.setUp();
    }

    public String escape(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); ++i) {
            char c = s.charAt(i);
            if (c == '\\' || c == '+' || c == '-' || c == '!' || c == '(' || c == ')' || c == ':' || c == '^' || c == '[' || c == ']' || c == '"' || c == '{' || c == '}' || c == '~' || c == '*' || c == '?' || c == '|' || c == '&' || c == '/') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * format es QueryString to lucene Phrase
     *
     * @param esQueryStringPharse "jakarta apache" AND "Apache Lucene"
     * @param analyzer
     * @return
     * @throws Exception
     */
    public String formatQueryStringToLucenePhrase(String esQueryStringPharse, Analyzer analyzer) throws Exception {
        Query query = parseQueryStringToLuceneQuery(esQueryStringPharse, fileNameMasks, analyzer);
        String lucenePharse;
        lucenePharse = query.toString().replaceAll(fileNameMask + ":", "");
        return lucenePharse;
    }

    /**
     * parse es QueryString to lucene Query
     *
     * @param esQueryStringPharse "jakarta apache" AND "Apache Lucene"
     * @param analyzer
     * @return
     * @throws Exception
     */
    public Query parseQueryStringToLuceneQuery(String esQueryStringPharse, List<String> filedNames, Analyzer analyzer) throws Exception {
        QueryStringQueryParser queryStringQueryParser = new QueryStringQueryParser(queryShardContext, filedNames.get(0));
        queryStringQueryParser.setForceAnalyzer(analyzer);
        queryStringQueryParser.setMultiFields(filedNames);
        Query query = queryStringQueryParser.parse(escape(esQueryStringPharse));
        return query;
    }

    /**
     * @param esQueryJson {
     *                    "range": {
     *                    "date": {
     *                    "gte": "2021-02-03T00:00:00",
     *                    "lte": "2021-02-04T00:00:00"
     *                    }
     *                    }
     *                    }
     * @return
     * @throws Exception
     */
    public Query parseNoAnalyzerToLuceneQuery(String esQueryJson) throws Exception {
        EsToLuceneQueryParser EsToLuceneQueryParser = new EsToLuceneQueryParser(queryShardContext.getXContentRegistry());
        org.elasticsearch.index.query.QueryBuilder queryBuilder = EsToLuceneQueryParser.parseQuery(esQueryJson);
        Query query = queryBuilder.toQuery(queryShardContext);
        return query;
    }

    /***
     *
     * @param esQueryJson
     * {
     *     "query": {
     *         "bool": {
     *             "must": [
     *               {
     *                     "query_string": {
     *                         "analyzer": "standard",
     *                         "query": "hello word",
     *                         "fields": [
     *                             "content",
     *                             "title"
     *                         ]
     *                     }
     *                 },
     *                 {
     *                     "terms": {
     *                         "domain": [
     *                             "www.github.com"
     *                         ]
     *                     }
     *                 },
     *                 {
     *                     "range": {
     *                         "date": {
     *                             "gte": "2021-02-03T00:00:00",
     *                             "lte": "2021-02-04T00:00:00"
     *                         }
     *                     }
     *                 }
     *             ]
     *         }
     *     }
     * }
     * @param filedNames
     * @param analyzer
     * @return
     * @throws Exception
     */
    public Query parseQueryToLuceneQuery(String esQueryJson, List<String> filedNames, Analyzer analyzer) throws Exception {
        QueryStringQueryParser.outMultiFields = filedNames;
        QueryStringQueryParser.outForceAnalyzer = analyzer;
        try (XContentParser parser = esToLuceneSearchParser.createParser(JsonXContent.jsonXContent, esQueryJson)) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
            return searchSourceBuilder.query().toQuery(queryShardContext);
        }
    }

    public Query parseQueryToLuceneQuery(String esQueryJson) throws Exception {
        return parseQueryToLuceneQuery(esQueryJson, null, null);
    }

    public String parseQueryToLucenePhrase(String esQueryJson, List<String> filedNames, Analyzer analyzer) throws Exception {
        Query query = parseQueryToLuceneQuery(esQueryJson, filedNames, analyzer);
        return query.toString();
    }

    public String parseQueryToLucenePhrase(String esQueryJson) throws Exception {
        return parseQueryToLucenePhrase(esQueryJson, null, null);
    }
}
