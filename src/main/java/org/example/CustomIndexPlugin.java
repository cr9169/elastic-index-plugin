package org.example;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class CustomIndexPlugin extends Plugin implements ActionPlugin {

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
        return Arrays.asList(
                new CustomIndexRestHandler(),
                new TxtProcessingRestHandler(),
                new CustomIndexRestHandlerManagingVersion()
        );
    }
}
