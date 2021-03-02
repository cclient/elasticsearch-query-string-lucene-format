package com.github.cclient.elasticsearch.format;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.SearchModule;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Function;


public class EsToLuceneSearchParser {

    protected NamedWriteableRegistry namedWriteableRegistry;
    private TestSearchExtPlugin searchExtPlugin;
    private NamedXContentRegistry xContentRegistry;

    public void setUp() throws Exception {
        IndicesModule indicesModule = new IndicesModule(Collections.emptyList());
        searchExtPlugin = new TestSearchExtPlugin();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.singletonList(searchExtPlugin));
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(indicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
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
//    protected final XContentParser createParser(XContent xContent, String data) throws IOException {
    public final XContentParser createParser(XContent xContent, String data) throws IOException {
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
     * The {@link NamedWriteableRegistry} to use for this test. Subclasses should override and use liberally.
     */
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    private static class TestSearchExtPlugin extends Plugin implements SearchPlugin {
        private final List<SearchExtSpec<? extends SearchExtBuilder>> searchExtSpecs;
        private final Map<String, Function<String, ? extends SearchExtBuilder>> supportedElements;

        private TestSearchExtPlugin() {
            int numSearchExts = 1;
            this.searchExtSpecs = new ArrayList<>(numSearchExts);
            this.supportedElements = new HashMap<>();
            if (this.supportedElements.put(TestSearchExtBuilder1.NAME, TestSearchExtBuilder1::new) == null) {
                this.searchExtSpecs.add(new SearchExtSpec<>(TestSearchExtBuilder1.NAME, TestSearchExtBuilder1::new,
                        new TestSearchExtParser<>(TestSearchExtBuilder1::new)));
            }
        }

        Map<String, Function<String, ? extends SearchExtBuilder>> getSupportedElements() {
            return supportedElements;
        }

        @Override
        public List<SearchExtSpec<?>> getSearchExts() {
            return searchExtSpecs;
        }
    }

    private static class TestSearchExtParser<T extends SearchExtBuilder> implements CheckedFunction<XContentParser, T, IOException> {
        private final Function<String, T> searchExtBuilderFunction;

        TestSearchExtParser(Function<String, T> searchExtBuilderFunction) {
            this.searchExtBuilderFunction = searchExtBuilderFunction;
        }

        @Override
        public T apply(XContentParser parser) throws IOException {
            return searchExtBuilderFunction.apply(parseField(parser));
        }

        String parseField(XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "start_object expected, found " + parser.currentToken());
            }
            if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(parser.getTokenLocation(), "field_name expected, found " + parser.currentToken());
            }
            String field = parser.currentName();
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "start_object expected, found " + parser.currentToken());
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "end_object expected, found " + parser.currentToken());
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "end_object expected, found " + parser.currentToken());
            }
            return field;
        }
    }

    //Would be nice to have a single builder that gets its name as a parameter, but the name wouldn't get a value when the object
    //is created reading from the stream (constructor that takes a StreamInput) which is a problem as we check that after reading
    //a named writeable its name is the expected one. That's why we go for the following less dynamic approach.
    private static class TestSearchExtBuilder1 extends TestSearchExtBuilder {
        private static final String NAME = "name1";

        TestSearchExtBuilder1(String field) {
            super(NAME, field);
        }

        TestSearchExtBuilder1(StreamInput in) throws IOException {
            super(NAME, in);
        }
    }

    private abstract static class TestSearchExtBuilder extends SearchExtBuilder {
        protected final String name;
        final String objectName;

        TestSearchExtBuilder(String name, String objectName) {
            this.name = name;
            this.objectName = objectName;
        }

        TestSearchExtBuilder(String name, StreamInput in) throws IOException {
            this.name = name;
            this.objectName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(objectName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestSearchExtBuilder that = (TestSearchExtBuilder) o;
            return Objects.equals(objectName, that.objectName) &&
                    Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(objectName, name);
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            builder.startObject(objectName);
            builder.endObject();
            builder.endObject();
            return builder;
        }
    }
}
