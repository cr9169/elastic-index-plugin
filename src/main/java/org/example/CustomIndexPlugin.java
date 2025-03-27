package org.example;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * CustomIndexPlugin is a custom Elasticsearch plugin that registers custom REST handlers.
 *
 * <p>This plugin registers two REST handlers:
 * <ul>
 *   <li>{@link CustomIndexRestHandler} - for handling direct JSON indexing requests.</li>
 *   <li>{@link TxtProcessingRestHandler} - for processing large TXT files.</li>
 * </ul>
 *
 * <p>By implementing {@code ActionPlugin}, the plugin is able to extend the REST API functionality
 * provided by Elasticsearch.
 */
public class CustomIndexPlugin extends Plugin implements ActionPlugin {

    /**
     * Returns the list of custom REST handlers to be registered with Elasticsearch.
     *
     * @param settings                 the node settings.
     * @param restController           the REST controller.
     * @param clusterSettings          the cluster settings.
     * @param indexScopedSettings      the index scoped settings.
     * @param settingsFilter           the settings filter.
     * @param indexNameExpressionResolver the index name expression resolver.
     * @param nodesInCluster           a supplier for the current discovery nodes in the cluster.
     * @return a list of {@link RestHandler} instances to be registered.
     */
    @Override
    public List<RestHandler> getRestHandlers(
            Settings settings,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {

        // Register two REST handlers: one for direct JSON indexing and one for processing TXT files.
        return Arrays.asList(
                new CustomIndexRestHandler(),
                new TxtProcessingRestHandler()
        );
    }
}
