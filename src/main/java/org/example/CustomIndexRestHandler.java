package org.example;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class CustomIndexRestHandler extends BaseRestHandler {

    private static final Logger logger = Logger.getLogger(CustomIndexRestHandler.class.getName());
    private static final String INDEX_NAME = "target_index";
    private static final AtomicInteger documentCounter = new AtomicInteger(0);
    private static long startTime = System.currentTimeMillis();

    @Override
    public String getName() {
        return "custom_index_rest_handler";
    }

    @Override
    public List<Route> routes() {
        // Registers the endpoint POST /_custom_index
        return Collections.singletonList(
                new Route(RestRequest.Method.POST, "/_custom_index")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Check if index exists; if not, create it with optimized settings and mapping
        if (!indexExists(client, INDEX_NAME)) {
            createOptimizedIndex(client, INDEX_NAME);
            startTime = System.currentTimeMillis(); // Reset timer when index is created
            documentCounter.set(0); // Reset counter when index is created
        }

        // Log the start of processing
        int currentDoc = documentCounter.incrementAndGet();
        long docStartTime = System.currentTimeMillis();
        logger.info("Starting to index document #" + currentDoc + " in " + INDEX_NAME);

        // Create an IndexRequest using the JSON content from the request body
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME);
        indexRequest.source(request.content(), request.getXContentType());

        // Pass the request to Elasticsearch's indexing mechanism with enhanced response handling
        return channel -> client.index(indexRequest, ActionListener.wrap(
                response -> {
                    long processingTime = System.currentTimeMillis() - docStartTime;
                    long totalProcessed = documentCounter.get();
                    long totalTime = System.currentTimeMillis() - startTime;
                    float docsPerSecond = (float) totalProcessed / (totalTime / 1000f);

                    logger.info("Document #" + currentDoc + " indexed successfully in " + processingTime + "ms. " +
                            "Total: " + totalProcessed + " docs in " + (totalTime / 1000) + "s (" + docsPerSecond + " docs/sec)");

                    try {
                        // Create an enhanced response with additional indexing stats
                        XContentBuilder builder = XContentFactory.jsonBuilder();
                        builder.startObject();

                        // Include original response data
                        builder.field("_index", response.getIndex());
                        builder.field("_id", response.getId());
                        builder.field("_version", response.getVersion());
                        builder.field("result", response.getResult().toString());
                        builder.field("_seq_no", response.getSeqNo());
                        builder.field("_primary_term", response.getPrimaryTerm());

                        // Add custom fields
                        builder.field("indexing_success", true);
                        builder.field("processing_time_ms", processingTime);
                        builder.field("document_number", currentDoc);
                        builder.field("total_processed", totalProcessed);
                        builder.field("docs_per_second", docsPerSecond);

                        builder.endObject();

                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    } catch (Exception e) {
                        logger.warning("Error creating enhanced response: " + e.getMessage());
                        new RestToXContentListener<>(channel).onResponse(response);
                    }
                },
                exception -> {
                    logger.warning("Failed to index document #" + currentDoc + ": " + exception.getMessage());
                    new RestToXContentListener<>(channel).onFailure(exception);
                }
        ));
    }

    /**
     * Checks if the specified index exists.
     *
     * @param client    the NodeClient instance
     * @param indexName the name of the index to check
     * @return true if the index exists, false otherwise
     */
    private boolean indexExists(NodeClient client, String indexName) {
        try {
            // Alternative approach using the cluster state API
            return client.admin()
                    .cluster()
                    .prepareState()
                    .execute()
                    .actionGet()
                    .getState()
                    .metadata()
                    .hasIndex(indexName);
        } catch (Exception e) {
            logger.warning("Error checking if index exists: " + e.getMessage());
            return false;
        }
    }

    /**
     * Creates the index with optimized settings and mapping.
     *
     * @param client    the NodeClient instance
     * @param indexName the name of the index to create
     * @throws IOException if an error occurs during index creation
     */
    private void createOptimizedIndex(NodeClient client, String indexName) throws IOException {
        // Create a new index with settings optimized for fast indexing
        CreateIndexRequest createRequest = new CreateIndexRequest(indexName);
        createRequest.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "30s") // Changed from 120s to 30s for faster verification
                .put("index.translog.durability", "async")
                .put("index.translog.flush_threshold_size", "4gb")
                .put("index.translog.sync_interval", "30s") // Changed from 120s to 30s
                .put("index.merge.scheduler.max_thread_count", 1)
                .put("index.merge.policy.segments_per_tier", 50)
                .put("index.merge.policy.max_merged_segment", "5gb")
                .put("index.indexing.slowlog.threshold.index.warn", "60s")
                .put("index.indexing.slowlog.threshold.index.info", "30s") // Changed from 60s to 30s
                .build());

        // Create the index first
        client.admin().indices().create(createRequest).actionGet();
        logger.info("Created optimized index '" + indexName + "'");

        // Then apply mappings separately
        try {
            XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("content")
                    .field("type", "text")
                    .field("index", true)
                    .field("doc_values", false)
                    .field("norms", false)
                    .endObject()
                    .startObject("fileIdentifier")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("fileName")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("sequenceNumber")
                    .field("type", "integer")
                    .endObject()
                    .endObject()
                    .endObject();

            client.admin()
                    .indices()
                    .preparePutMapping(indexName)
                    .setSource(mappingBuilder)
                    .get();

            logger.info("Applied optimized mapping for '" + indexName + "'");
        } catch (Exception e) {
            logger.warning("Failed to apply optimized mapping: " + e.getMessage());
        }
    }
}