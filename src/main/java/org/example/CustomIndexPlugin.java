package org.example;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Main entry point for the custom Elasticsearch plugin.
 * <p>
 * This plugin registers custom REST API handlers to extend Elasticsearch’s functionality.
 * It implements {@link ActionPlugin} to hook into Elasticsearch's REST action system.
 */
public class CustomIndexPlugin extends Plugin implements ActionPlugin {

    /**
     * Registers the custom REST API handlers for this plugin.
     *
     * @param settings                    The current node settings.
     * @param namedWriteableRegistry      Registry for wire-serializable objects.
     * @param restController              Used to register REST routes.
     * @param clusterSettings             Cluster-level dynamic settings.
     * @param indexScopedSettings         Index-level dynamic settings.
     * @param settingsFilter              Used to filter sensitive settings.
     * @param indexNameExpressionResolver Helps resolve index patterns.
     * @param nodesInCluster              Supplier for accessing current cluster nodes.
     * @param clusterSupportsFeature      Predicate to check if the cluster supports certain features.
     * @return A collection of {@link RestHandler} instances that define new REST endpoints.
     */
    @Override
    public Collection<RestHandler> getRestHandlers(
            Settings settings,
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature
    ) {
        // Register all custom REST handlers for this plugin
        return Arrays.asList(
                new CustomIndexRestHandler(),
                new TxtProcessingStreamHandler(),
//                new TxtProcessingRestHandler(),
                new CustomIndexRestHandlerManagingVersion()
        );
    }
}
