package org.elasticsearch.test;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.Version;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchModule;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class AbstractBuilderTestCase {

    public static final String STRING_FIELD_NAME = "mapped_string";
    public static final String STRING_ALIAS_FIELD_NAME = "mapped_string_alias";
    protected static final String STRING_FIELD_NAME_2 = "mapped_string_2";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String INT_ALIAS_FIELD_NAME = "mapped_int_field_alias";
    protected static final String INT_RANGE_FIELD_NAME = "mapped_int_range";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_FIELD_NAME = "mapped_date";
    protected static final String DATE_ALIAS_FIELD_NAME = "mapped_date_alias";
    protected static final String DATE_RANGE_FIELD_NAME = "mapped_date_range";
    protected static final String OBJECT_FIELD_NAME = "mapped_object";
    protected static final String GEO_POINT_FIELD_NAME = "mapped_geo_point";
    protected static final String GEO_POINT_ALIAS_FIELD_NAME = "mapped_geo_point_alias";
    protected static final String GEO_SHAPE_FIELD_NAME = "mapped_geo_shape";
    protected static final String[] MAPPED_FIELD_NAMES = new String[]{STRING_FIELD_NAME, STRING_ALIAS_FIELD_NAME,
            INT_FIELD_NAME, INT_RANGE_FIELD_NAME, DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, DATE_FIELD_NAME,
            DATE_RANGE_FIELD_NAME, OBJECT_FIELD_NAME, GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME,
            GEO_SHAPE_FIELD_NAME};
    protected static final String[] MAPPED_LEAF_FIELD_NAMES = new String[]{STRING_FIELD_NAME, STRING_ALIAS_FIELD_NAME,
            INT_FIELD_NAME, INT_RANGE_FIELD_NAME, DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME,
            DATE_FIELD_NAME, DATE_RANGE_FIELD_NAME, GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME};

    private static final Map<String, String> ALIAS_TO_CONCRETE_FIELD_NAME = new HashMap<>();
    public static ServiceHolder serviceHolder;
    protected static String[] randomTypes;
    private static int queryNameId = 0;
    private static Settings nodeSettings;
    private static Index index;
    private static String[] currentTypes;
    private static long nowInMillis;

    static {
        ALIAS_TO_CONCRETE_FIELD_NAME.put(STRING_ALIAS_FIELD_NAME, STRING_FIELD_NAME);
        ALIAS_TO_CONCRETE_FIELD_NAME.put(INT_ALIAS_FIELD_NAME, INT_FIELD_NAME);
        ALIAS_TO_CONCRETE_FIELD_NAME.put(DATE_ALIAS_FIELD_NAME, DATE_FIELD_NAME);
        ALIAS_TO_CONCRETE_FIELD_NAME.put(GEO_POINT_ALIAS_FIELD_NAME, GEO_POINT_FIELD_NAME);
    }

    static {
        nodeSettings = Settings.builder()
                .put("node.name", AbstractBuilderTestCase.class.toString())
                .put(Environment.PATH_HOME_SETTING.getKey(), "../")
                .build();

        index = new Index("cclient_test_index", "_na_");
        nowInMillis = 10086;

        currentTypes = new String[]{"_doc"};
        randomTypes = new String[]{"123", "456"};
    }

    /**
     * @return a new {@link QueryShardContext} with the provided reader
     */
    public static QueryShardContext createShardContext(IndexReader reader) {
        return serviceHolder.createShardContext(reader);
    }

    /**
     * @return a new {@link QueryShardContext} based on the base test index and queryParserService
     */
    public static QueryShardContext createShardContext() {
        return createShardContext(null);
    }

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.emptyList();
    }

    protected Settings createTestIndexSettings() {
        Version indexVersionCreated = Version.CURRENT;
        return Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, indexVersionCreated)
                .build();
    }

    public void beforeTest() throws Exception {
        if (serviceHolder == null) {
            serviceHolder = new ServiceHolder(nodeSettings, createTestIndexSettings(), getPlugins(), nowInMillis,
                    AbstractBuilderTestCase.this);
        }
    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers / builders
     */
    protected GetResponse executeGet(GetRequest getRequest) {
        throw new UnsupportedOperationException("this test can't handle GET requests");
    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers / builders
     */
    protected MultiTermVectorsResponse executeMultiTermVectors(MultiTermVectorsRequest mtvRequest) {
        throw new UnsupportedOperationException("this test can't handle MultiTermVector requests");
    }

    private static class ServiceHolder implements Closeable {
        private final SearchModule searchModule;
        private final NamedWriteableRegistry namedWriteableRegistry;
        private final NamedXContentRegistry xContentRegistry;
        private final IndexSettings idxSettings;
        private final SimilarityService similarityService;
        private final MapperService mapperService;
        private final long nowInMillis;

        ServiceHolder(Settings nodeSettings, Settings indexSettings, Collection<Class<? extends Plugin>> plugins, long nowInMillis,
                      AbstractBuilderTestCase testCase) throws IOException {
            Environment env = InternalSettingsPreparer.prepareEnvironment(nodeSettings, null);
            this.nowInMillis = nowInMillis;
            PluginsService pluginsService;
            pluginsService = new PluginsService(nodeSettings, null, env.modulesFile(), env.pluginsFile(), plugins);

            List<Setting<?>> additionalSettings = pluginsService.getPluginSettings();
            SettingsModule settingsModule =
                    new SettingsModule(nodeSettings, additionalSettings, pluginsService.getPluginSettingsFilter(), Collections.emptySet());
            searchModule = new SearchModule(nodeSettings, false, pluginsService.filterPlugins(SearchPlugin.class));
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
            entries.addAll(indicesModule.getNamedWriteables());
            entries.addAll(searchModule.getNamedWriteables());
            namedWriteableRegistry = new NamedWriteableRegistry(entries);
            xContentRegistry = new NamedXContentRegistry(Stream.of(
                    searchModule.getNamedXContents().stream()
            ).flatMap(Function.identity()).collect(toList()));
            IndexScopedSettings indexScopedSettings = settingsModule.getIndexScopedSettings();
            idxSettings = IndexSettingsModule.newIndexSettings(index, indexSettings, indexScopedSettings);
            AnalysisModule analysisModule = new AnalysisModule(TestEnvironment.newEnvironment(nodeSettings), emptyList());
            IndexAnalyzers indexAnalyzers = analysisModule.getAnalysisRegistry().build(idxSettings);
            similarityService = new SimilarityService(idxSettings, null, Collections.emptyMap());
            MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
            mapperService = new MapperService(idxSettings, indexAnalyzers, xContentRegistry, similarityService, mapperRegistry,
                    () -> createShardContext(null));
            IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(nodeSettings, new IndexFieldDataCache.Listener() {
            });
        }

        @Override
        public void close() throws IOException {
        }

        public QueryShardContext createShardContext(IndexReader reader) {
            return new QueryShardContext(0, idxSettings, null, null, mapperService,
                    null, null, xContentRegistry, namedWriteableRegistry, null, reader, () -> nowInMillis, null);

        }

    }

}