package org.example;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Custom REST handler for indexing documents into a performance-optimized index.
 * <p>
 * Endpoint: POST /_custom_index
 */
public class CustomIndexRestHandler extends BaseRestHandler {

    private static final Logger logger = Logger.getLogger(CustomIndexRestHandler.class.getName());
    private static final String INDEX_NAME = "target_index";
    private static final AtomicInteger documentCounter = new AtomicInteger(0);
    private static long startTime = System.currentTimeMillis();

    /**
     * Returns a unique name for this handler.
     */
    @Override
    public String getName() {
        return "custom_index_rest_handler";
    }

    /**
     * Registers the POST /_custom_index endpoint.
     */
    @Override
    public List<Route> routes() {
        return Collections.singletonList(
                new Route(RestRequest.Method.POST, "/_custom_index")
        );
    }

    /**
     * Prepares and processes the indexing request.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!indexExists(client, INDEX_NAME)) {
            createOptimizedIndex(client, INDEX_NAME);
            startTime = System.currentTimeMillis();
            documentCounter.set(0);
        }

        int currentDoc = documentCounter.incrementAndGet();
        long docStartTime = System.currentTimeMillis();
        logger.info("Starting to index document #" + currentDoc + " in " + INDEX_NAME);

        IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
                .source(request.content(), request.getXContentType());

        return channel -> client.index(indexRequest, ActionListener.wrap(
                response -> {
                    long processingTime = System.currentTimeMillis() - docStartTime;
                    long totalProcessed = documentCounter.get();
                    long totalTime = System.currentTimeMillis() - startTime;
                    float docsPerSecond = (float) totalProcessed / (totalTime / 1000f);

                    logger.info("Document #" + currentDoc + " indexed successfully in " + processingTime + "ms. " +
                            "Total: " + totalProcessed + " docs in " + (totalTime / 1000) + "s (" + docsPerSecond + " docs/sec)");

                    try {
                        XContentBuilder builder = XContentFactory.jsonBuilder()
                                .startObject()
                                .field("_index", response.getIndex())
                                .field("_id", response.getId())
                                .field("_version", response.getVersion())
                                .field("result", response.getResult().toString())
                                .field("_seq_no", response.getSeqNo())
                                .field("_primary_term", response.getPrimaryTerm())
                                .field("indexing_success", true)
                                .field("processing_time_ms", processingTime)
                                .field("document_number", currentDoc)
                                .field("total_processed", totalProcessed)
                                .field("docs_per_second", docsPerSecond)
                                .endObject();

                        channel.sendResponse(new RestResponse(RestStatus.OK, builder));
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
     * Checks if the specified index exists in the cluster.
     */
    private boolean indexExists(NodeClient client, String indexName) {
        try {
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
     * Creates a new index with optimized settings and mappings.
     */
    private void createOptimizedIndex(NodeClient client, String indexName) throws IOException {
        CreateIndexRequest createRequest = new CreateIndexRequest(indexName);
        createRequest.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "30s")
                .put("index.translog.durability", "async")
                .put("index.translog.flush_threshold_size", "4gb")
                .put("index.translog.sync_interval", "30s")
                .put("index.merge.scheduler.max_thread_count", 1)
                .put("index.merge.policy.segments_per_tier", 50)
                .put("index.merge.policy.max_merged_segment", "5gb")
                .put("index.indexing.slowlog.threshold.index.warn", "60s")
                .put("index.indexing.slowlog.threshold.index.info", "30s")
                .build());

        client.admin().indices().create(createRequest).actionGet();
        logger.info("Created optimized index '" + indexName + "'");

        try {
            XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("content").field("type", "text").field("index", true).field("doc_values", false).field("norms", false).endObject()
                    .startObject("fileIdentifier").field("type", "keyword").endObject()
                    .startObject("fileName").field("type", "keyword").endObject()
                    .startObject("sequenceNumber").field("type", "integer").endObject()
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