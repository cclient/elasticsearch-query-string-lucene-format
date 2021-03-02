package com.github.cclient.elasticsearch.format;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;

import java.io.IOException;
import java.io.InputStream;

import static java.util.Collections.emptyList;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class EsToLuceneQueryParser {

    private NamedXContentRegistry namedXContentRegistry;

    public EsToLuceneQueryParser() {
    }

    public EsToLuceneQueryParser(NamedXContentRegistry namedXContentRegistry) {
        this.namedXContentRegistry = namedXContentRegistry;
    }

    protected static QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder parseInnerQueryBuilder = parseInnerQueryBuilder(parser);
//        assertNull(parser.nextToken());
        return parseInnerQueryBuilder;
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContentBuilder builder) throws IOException {
        return builder.generator().contentType().xContent()
                .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, String data) throws IOException {
        return xContent.createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, InputStream data) throws IOException {
        return xContent.createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, byte[] data) throws IOException {
        return xContent.createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, BytesReference data) throws IOException {
        return xContent.createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, data.streamInput());
    }

    /**
     * The {@link NamedXContentRegistry} to use for this test. Subclasses should override and use liberally.
     */
    protected NamedXContentRegistry xContentRegistry() {
//        return new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        if (this.namedXContentRegistry == null) {
            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
//            namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
            return new NamedXContentRegistry(searchModule.getNamedXContents());
//            return new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        }
        return this.namedXContentRegistry;
    }

    protected QueryBuilder parseQuery(AbstractQueryBuilder<?> builder) throws IOException {
        BytesReference bytes = XContentHelper.toXContent(builder, XContentType.JSON, false);
        return parseQuery(createParser(JsonXContent.jsonXContent, bytes));
    }

    //    protected QueryBuilder parseQuery(String queryAsString) throws IOException {
    public QueryBuilder parseQuery(String queryAsString) throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, queryAsString);
        return parseQuery(parser);
    }

    public void setNamedXContentRegistry(NamedXContentRegistry namedXContentRegistry) {
        this.namedXContentRegistry = namedXContentRegistry;
    }
}
