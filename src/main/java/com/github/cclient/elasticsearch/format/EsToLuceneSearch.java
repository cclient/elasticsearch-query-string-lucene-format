package com.github.cclient.elasticsearch.format;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EsToLuceneSearch {

    public static Directory createRAMIndex(List<Map<String, Object>> docs, Analyzer analyzer, List<String> fields) throws IOException {
        ByteBuffersDirectory d = new ByteBuffersDirectory();
        IndexWriterConfig conf = new IndexWriterConfig(analyzer);
        conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        IndexWriter indexWriter = new IndexWriter(d, conf);
        System.out.println("docs :" + docs);
        System.out.println("fields :" + fields);
        for (int i = 0; i < docs.size(); i++) {
            Map<String, Object> json = docs.get(i);
            Document document = new Document();
            document.add(new StringField("_id", (String) json.get("_id"), Field.Store.YES));
            for (String field : fields) {
                if (json.containsKey(field)) {
                    if (field.startsWith("text_")) {
                        document.add(new TextField(field, (String) json.get(field), Field.Store.NO));
                    }
                    if (field.startsWith("long_")) {
                        document.add(new TextField(field, json.get(field).toString(), Field.Store.NO));
                    }
                }
            }
            try {
                indexWriter.addDocument(document);
            } catch (IllegalArgumentException ex) {
                System.out.println("error createRAMIndex: " + ex);
            }
        }
        indexWriter.commit();
        indexWriter.close();
        return d;
    }

    public static List<String> filter(Directory d, Query query, int maxCount) throws IOException {
        IndexReader r = DirectoryReader.open(d);
        IndexSearcher indexSearcher = new IndexSearcher(r);
        TopDocs topDocs = indexSearcher.search(query, maxCount);
        List<String> list = new ArrayList<>();
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            int docId = scoreDoc.doc;
            Document doc = indexSearcher.doc(docId);
            Explanation explanation = indexSearcher.explain(query, docId);
            System.out.println("explanation: " + explanation);
            System.out.println("doc: " + doc);
            list.add(doc.get("_id"));
        }
        return list;
    }

}
