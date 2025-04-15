package org.example;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestResponse;
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
        // רישום ה-endpoint POST /_custom_index
        return Collections.singletonList(
                new Route(RestRequest.Method.POST, "/_custom_index")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // בדיקת קיום האינדקס; אם אינו קיים – צור אותו עם הגדרות ומיפוי מותאמים
        if (!indexExists(client, INDEX_NAME)) {
            createOptimizedIndex(client, INDEX_NAME);
            startTime = System.currentTimeMillis(); // אתחול טיימר כאשר האינדקס נוצר
            documentCounter.set(0); // אתחול המונה כאשר האינדקס נוצר
        }

        // רישום התחלת העיבוד
        int currentDoc = documentCounter.incrementAndGet();
        long docStartTime = System.currentTimeMillis();
        logger.info("Starting to index document #" + currentDoc + " in " + INDEX_NAME);

        // יצירת IndexRequest באמצעות תוכן ה-JSON שנשלח בגוף הבקשה
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME);
        indexRequest.source(request.content(), request.getXContentType());

        // העברת הבקשה למנגנון האינדוקס של Elasticsearch עם טיפול בתגובה משודרג
        return channel -> client.index(indexRequest, ActionListener.wrap(
                response -> {
                    long processingTime = System.currentTimeMillis() - docStartTime;
                    long totalProcessed = documentCounter.get();
                    long totalTime = System.currentTimeMillis() - startTime;
                    float docsPerSecond = (float) totalProcessed / (totalTime / 1000f);

                    logger.info("Document #" + currentDoc + " indexed successfully in " + processingTime + "ms. " +
                            "Total: " + totalProcessed + " docs in " + (totalTime / 1000) + "s (" + docsPerSecond + " docs/sec)");

                    try {
                        // יצירת תגובה משודרגת עם סטטיסטיקות אינדוקס נוספות
                        XContentBuilder builder = XContentFactory.jsonBuilder();
                        builder.startObject();

                        // הכללת נתוני התגובה המקוריים
                        builder.field("_index", response.getIndex());
                        builder.field("_id", response.getId());
                        builder.field("_version", response.getVersion());
                        builder.field("result", response.getResult().toString());
                        builder.field("_seq_no", response.getSeqNo());
                        builder.field("_primary_term", response.getPrimaryTerm());

                        // הוספת שדות מותאמים אישית
                        builder.field("indexing_success", true);
                        builder.field("processing_time_ms", processingTime);
                        builder.field("document_number", currentDoc);
                        builder.field("total_processed", totalProcessed);
                        builder.field("docs_per_second", docsPerSecond);

                        builder.endObject();

                        // שימוש ב-RestResponse במקום BytesRestResponse
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
     * בודק האם האינדקס הנתון קיים.
     *
     * @param client    מופע NodeClient
     * @param indexName שם האינדקס לבדיקה
     * @return true אם האינדקס קיים, false אחרת
     */
    private boolean indexExists(NodeClient client, String indexName) {
        try {
            // גישה למצב הקלסטר ובדיקה האם המיפוי כולל את האינדקס
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
     * יוצר את האינדקס עם הגדרות ומיפוי מותאמים לאופטימיזציה.
     *
     * @param client    מופע NodeClient
     * @param indexName שם האינדקס ליצירה
     * @throws IOException במקרה של שגיאה ביצירת האינדקס
     */
    private void createOptimizedIndex(NodeClient client, String indexName) throws IOException {
        // יצירת אינדקס חדש עם הגדרות מותאמות לאינדוקס מהיר
        CreateIndexRequest createRequest = new CreateIndexRequest(indexName);
        createRequest.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "30s") // שינוי מ-120s ל-30s לאימות מהיר יותר
                .put("index.translog.durability", "async")
                .put("index.translog.flush_threshold_size", "4gb")
                .put("index.translog.sync_interval", "30s") // שינוי מ-120s ל-30s
                .put("index.merge.scheduler.max_thread_count", 1)
                .put("index.merge.policy.segments_per_tier", 50)
                .put("index.merge.policy.max_merged_segment", "5gb")
                .put("index.indexing.slowlog.threshold.index.warn", "60s")
                .put("index.indexing.slowlog.threshold.index.info", "30s") // שינוי מ-60s ל-30s
                .build());

        // יצירת האינדקס
        client.admin().indices().create(createRequest).actionGet();
        logger.info("Created optimized index '" + indexName + "'");

        // החלפת המיפוי בנפרד
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
