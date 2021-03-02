package com.github.cclient.elasticsearch.format;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.Query;
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
        String withExistsAnalyzer = "{\n" +
                "    \"query\": {\n" +
                "        \"bool\": {\n" +
                "            \"must\": [\n" +
                "                {\n" +
                "                    \"query_string\": {\n" +
                "                        \"analyzer\": \"standard\",\n" +
                "                        \"query\": \"(Git code guide) OR (repository branch)\",\n" +
                "                        \"fields\": [\n" +
                "                            \"text_body\"\n" +
                "                        ]\n" +
                "                    }\n" +
                "                },\n" +
                "                {\n" +
                "                    \"range\": {\n" +
                "                        \"long_view\": {\n" +
                "                            \"gte\": \"0\",\n" +
                "                            \"lte\": \"1000\"\n" +
                "                        }\n" +
                "                    }\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        try {
            EsToLucene esToLucene = new EsToLucene();
            Query query = esToLucene.parseQueryToLuceneQuery(withExistsAnalyzer, Arrays.asList("text_body"), new StandardAnalyzer());
            System.out.println("query: " + query.toString());
            List<String> matchIds = EsToLuceneSearch.filter(directory, query, 100);
            System.out.println("matchIds: " + matchIds);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}