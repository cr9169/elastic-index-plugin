    package org.example;

    import org.elasticsearch.action.ActionListener;
    import org.elasticsearch.action.index.IndexRequest;
    import org.elasticsearch.client.internal.node.NodeClient;
    import org.elasticsearch.rest.*;
    import org.elasticsearch.xcontent.XContentBuilder;
    import org.elasticsearch.xcontent.XContentFactory;
    import org.elasticsearch.xcontent.XContentParser;
    import org.elasticsearch.xcontent.XContentType;
    import org.elasticsearch.xcontent.NamedXContentRegistry;
    import org.elasticsearch.xcontent.DeprecationHandler;
    import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
    import org.elasticsearch.cluster.health.ClusterHealthStatus;
    import org.elasticsearch.core.TimeValue;
    import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
    import org.elasticsearch.action.bulk.BulkRequest;
    import org.elasticsearch.action.bulk.BulkResponse;
    import org.elasticsearch.common.settings.Settings;
    import java.util.concurrent.*;
    import java.util.concurrent.atomic.AtomicBoolean;

    import java.io.*;
    import java.lang.management.ManagementFactory;
    import java.lang.management.MemoryMXBean;
    import java.lang.management.MemoryUsage;
    import java.net.HttpURLConnection;
    import java.net.Socket;
    import java.net.URL;
    import java.nio.charset.StandardCharsets;
    import java.util.*;
    import java.util.concurrent.atomic.AtomicInteger;
    import java.util.logging.Logger;
    import java.util.logging.Level;

    import com.sun.management.OperatingSystemMXBean;

    public class CustomIndexRestHandlerManagingVersion extends BaseRestHandler {

        private static final Logger logger = Logger.getLogger(CustomIndexRestHandlerManagingVersion.class.getName());
        private static final String INDEX_NAME = "target_index";
        private static final String DOTNET_SERVICE_URL = "http://inf-parseit-poc-service.elastic-system.svc.cluster.local:5203/api/Txt/pluginManager/processFile";
        private static final AtomicInteger documentCounter = new AtomicInteger(0);
        private static final int BATCH_SIZE = 30;
        private static final int MAX_CONCURRENT_BATCHES = 5;
        private static final int MAX_PARALLELISM = 4;

        @Override
        public String getName() {
            return "custom_index_rest_handler_managing_version";
        }

        @Override
        public List<Route> routes() {
            return Collections.singletonList(
                    new Route(RestRequest.Method.POST, "/managed_process_txt")
            );
        }


        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            // ─── Overall metrics start ───────────────────────────────────────────────
            long overallStart      = System.currentTimeMillis();
            double cpuOverallStart = getProcessCpuLoadPercent();
            double heapOverallStart = getHeapUsedPercent();
            logger.info(String.format("[CPU] Overall | Phase: Start | Process CPU Load: %.2f%%", cpuOverallStart));
            logger.info(String.format("[HEAP] Overall | Phase: Start | Heap Used: %.2f%%", heapOverallStart));

            // ─── Parse JSON payload safely ────────────────────────────────────────────
            Map<String, Object> payload;
            String filePath;
            try {
                payload  = request.contentParser().map();
                filePath = (String) payload.get("path");
                if (filePath == null || filePath.isBlank()) {
                    throw new IllegalArgumentException("Missing or blank 'path' field");
                }
            } catch (Exception e) {
                return channel -> {
                    XContentBuilder b = XContentFactory.jsonBuilder()
                            .startObject()
                            .field("error", "Invalid request JSON: " + e.getMessage())
                            .endObject();
                    channel.sendResponse(new RestResponse(RestStatus.BAD_REQUEST, b));
                };
            }
            logger.info("[PLUGIN] Received file processing request for path: " + filePath);

            // ─── Actual processing consumer ───────────────────────────────────────────
            return channel -> {
                // STEP 1: Request Chunks from .NET Service
                double cpuDotnetStart = getProcessCpuLoadPercent();
                double heapDotnetStart = getHeapUsedPercent();
                long requestDotnetStart = System.currentTimeMillis();
                logger.info(String.format("[CPU] Step: Request .NET | Phase: Start | Process CPU Load: %.2f%%", cpuDotnetStart));
                logger.info(String.format("[HEAP] Step: Request .NET | Phase: Start | Heap Used: %.2f%%", heapDotnetStart));

                List<Map<String, Object>> chunks = requestChunksFromDotnet(filePath);

                double cpuDotnetEnd = getProcessCpuLoadPercent();
                double heapDotnetEnd = getHeapUsedPercent();
                long requestDotnetDuration = System.currentTimeMillis() - requestDotnetStart;
                double cpuDotnetAvg = (cpuDotnetStart + cpuDotnetEnd) / 2;
                double heapDotnetAvg = (heapDotnetStart + heapDotnetEnd) / 2;
                logger.info(String.format("[CPU] Step: Request .NET | Phase: End | Process CPU Load: %.2f%%", cpuDotnetEnd));
                logger.info(String.format("[HEAP] Step: Request .NET | Phase: End | Heap Used: %.2f%%", heapDotnetEnd));
                // time of processing in seconds (from sending request to getting chunks) including reading.
                logger.info(String.format("[CPU] Step: Request .NET | Phase: Avg | Duration: %.2fs | Avg CPU: %.2f%%",
                        requestDotnetDuration / 1000.0, cpuDotnetAvg));
                logger.info(String.format("[HEAP] Request .NET | Start: %.2f%% | End: %.2f%% | Avg: %.2f%%",
                        heapDotnetStart, heapDotnetEnd, heapDotnetAvg));

                if (chunks == null || chunks.isEmpty()) {
                    XContentBuilder b = XContentFactory.jsonBuilder()
                            .startObject()
                            .field("error", "Failed to receive chunks from .NET service")
                            .endObject();
                    channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, b));
                    return;
                }
                logger.info("[PLUGIN] Received " + chunks.size() + " chunks from .NET service");

                // STEP 2: Prepare the optimized index
                try {
                    prepareOptimizedIndex(client);
                } catch (Exception e) {
                    logger.warning("[PLUGIN] Could not optimize index settings: " + e.getMessage());
                }

                // STEP 3: Bulk Indexing - Using parallel bulk indexing
                double cpuIndexingStart = getProcessCpuLoadPercent();
                double heapIndexingStart = getHeapUsedPercent();
                long indexingStart = System.currentTimeMillis();
                logger.info(String.format("[CPU] Step: Bulk Indexing | Phase: Start | Process CPU Load: %.2f%%", cpuIndexingStart));
                logger.info(String.format("[HEAP] Step: Bulk Indexing | Phase: Start | Heap Used: %.2f%%", heapIndexingStart));

                // Use the new parallel bulk indexing method
                CompletableFuture<Boolean> bulkFuture = bulkIndexChunksParallel(chunks, client);
                boolean bulkResult;
                try {
                    bulkResult = bulkFuture.get(); // Wait for parallel indexing to complete
                    if (!bulkResult) {
                        XContentBuilder b = XContentFactory.jsonBuilder()
                                .startObject()
                                .field("error", "Bulk indexing failed")
                                .endObject();
                        channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, b));
                        return;
                    }
                } catch (Exception e) {
                    logger.severe("[PLUGIN] Error in bulk indexing process: " + e.getMessage());
                    XContentBuilder b = XContentFactory.jsonBuilder()
                            .startObject()
                            .field("error", "Error in bulk indexing process: " + e.getMessage())
                            .endObject();
                    channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, b));
                    return;
                }

                double cpuIndexingEnd = getProcessCpuLoadPercent();
                double heapIndexingEnd = getHeapUsedPercent();
                long indexingDuration = System.currentTimeMillis() - indexingStart;
                double cpuIndexingAvg = (cpuIndexingStart + cpuIndexingEnd) / 2;
                double heapIndexingAvg = (heapIndexingStart + heapIndexingEnd) / 2;
                logger.info(String.format("[CPU] Step: Bulk Indexing | Phase: End | Process CPU Load: %.2f%%", cpuIndexingEnd));
                logger.info(String.format("[HEAP] Step: Bulk Indexing | Phase: End | Heap Used: %.2f%%", heapIndexingEnd));
                logger.info(String.format("[CPU] Step: Bulk Indexing | Phase: Avg | Duration: %.2fs | Avg CPU: %.2f%%",
                        indexingDuration / 1000.0, cpuIndexingAvg));
                logger.info(String.format("[HEAP] Bulk Indexing | Start: %.2f%% | End: %.2f%% | Avg: %.2f%%",
                        heapIndexingStart, heapIndexingEnd, heapIndexingAvg));

                // STEP 3: Refresh Index
                double cpuRefreshStart = getProcessCpuLoadPercent();
                double heapRefreshStart = getHeapUsedPercent();
                long refreshStart = System.currentTimeMillis();
                logger.info(String.format("[CPU] Step: Refresh | Phase: Start | Process CPU Load: %.2f%%", cpuRefreshStart));
                logger.info(String.format("[HEAP] Step: Refresh | Phase: Start | Heap Used: %.2f%%", heapRefreshStart));

                try {
                    // במקום לבצע בקשת HTTP חיצונית, משתמשים ב-API ישירות
                    client.admin().indices().prepareRefresh(INDEX_NAME).execute().actionGet();
                    logger.info("[PLUGIN] Index refresh completed for " + INDEX_NAME);
                } catch (Exception e) {
                    logger.warning("Failed to refresh index: " + e.getMessage());
                }

                double cpuRefreshEnd = getProcessCpuLoadPercent();
                double heapRefreshEnd = getHeapUsedPercent();
                long refreshDuration = System.currentTimeMillis() - refreshStart;
                double cpuRefreshAvg = (cpuRefreshStart + cpuRefreshEnd) / 2;
                double heapRefreshAvg = (heapRefreshStart + heapRefreshEnd) / 2;
                logger.info(String.format("[CPU] Step: Refresh | Phase: End | Process CPU Load: %.2f%%", cpuRefreshEnd));
                logger.info(String.format("[HEAP] Step: Refresh | Phase: End | Heap Used: %.2f%%", heapRefreshEnd));
                logger.info(String.format("[CPU] Step: Refresh | Phase: Avg | Duration: %.2fs | Avg CPU: %.2f%%",
                        refreshDuration / 1000.0, cpuRefreshAvg));
                logger.info(String.format("[HEAP] Refresh | Start: %.2f%% | End: %.2f%% | Avg: %.2f%%",
                        heapRefreshStart, heapRefreshEnd, heapRefreshAvg));

                // STEP 4: Count Documents
                double cpuCountStart = getProcessCpuLoadPercent();
                double heapCountStart = getHeapUsedPercent();
                long countStart = System.currentTimeMillis();
                logger.info(String.format("[CPU] Step: Count | Phase: Start | Process CPU Load: %.2f%%", cpuCountStart));
                logger.info(String.format("[HEAP] Step: Count | Phase: Start | Heap Used: %.2f%%", heapCountStart));

                int indexedCount;
                try {
                    // במקום לבצע בקשת HTTP חיצונית, משתמשים ב-API ישירות
                    long count = client.prepareSearch(INDEX_NAME)
                            .setSize(0) // אין צורך להחזיר תוצאות, רק ספירה
                            .get()
                            .getHits()
                            .getTotalHits()
                            .value;
                    logger.info("[PLUGIN] Document count: " + count);
                    indexedCount = (int) count;
                } catch (Exception e) {
                    logger.warning("Failed to count documents: " + e.getMessage());
                    indexedCount = -1;
                }

                double cpuCountEnd = getProcessCpuLoadPercent();
                double heapCountEnd = getHeapUsedPercent();
                long countDuration = System.currentTimeMillis() - countStart;
                double cpuCountAvg = (cpuCountStart + cpuCountEnd) / 2;
                double heapCountAvg = (heapCountStart + heapCountEnd) / 2;
                logger.info(String.format("[CPU] Step: Count | Phase: End | Process CPU Load: %.2f%%", cpuCountEnd));
                logger.info(String.format("[HEAP] Step: Count | Phase: End | Heap Used: %.2f%%", heapCountEnd));
                logger.info(String.format("[CPU] Step: Count | Phase: Avg | Duration: %.2fs | Avg CPU: %.2f%%",
                        countDuration / 1000.0, cpuCountAvg));
                logger.info(String.format("[HEAP] Count | Start: %.2f%% | End: %.2f%% | Avg: %.2f%%",
                        heapCountStart, heapCountEnd, heapCountAvg));

                // STEP 5: Validation - בדיקת בריאות האינדקס והבטחת זמינות כל המסמכים
                double cpuValidationStart = getProcessCpuLoadPercent();
                double heapValidationStart = getHeapUsedPercent();
                long validationStart = System.currentTimeMillis();
                logger.info(String.format("[CPU] Step: Validation | Phase: Start | Process CPU Load: %.2f%%", cpuValidationStart));
                logger.info(String.format("[HEAP] Step: Validation | Phase: Start | Heap Used: %.2f%%", heapValidationStart));

                boolean validationSuccess = true;
                String validationMessage = "Index validation completed successfully";

                // בדיקה 1: ריענון מלא של האינדקס (flush) כדי לוודא שכל הנתונים נכתבו לדיסק
                try {
                    logger.info("[PLUGIN] Performing full index flush to ensure all data is committed to disk");
                    client.admin().indices().prepareFlush(INDEX_NAME).execute().actionGet();
                    logger.info("[PLUGIN] Index flush completed successfully");
                } catch (Exception e) {
                    logger.warning("[PLUGIN] Error during index flush: " + e.getMessage());
                }

                // בדיקה 2: בדיקת בריאות הקלאסטר (Cluster Health)
                boolean clusterHealthOk = false;
                try {
                    logger.info("[PLUGIN] Checking cluster health status");
                    ClusterHealthResponse healthResponse = client.admin()
                            .cluster()
                            .prepareHealth(INDEX_NAME)
                            .setWaitForYellowStatus()  // לפחות YELLOW כדי שהאינדקס יהיה זמין לחיפוש
                            .setTimeout(TimeValue.timeValueSeconds(30))
                            .execute()
                            .actionGet();

                    if (healthResponse.getStatus() == ClusterHealthStatus.GREEN) {
                        logger.info("[PLUGIN] Cluster health is GREEN for " + INDEX_NAME + ". Index is fully ready.");
                        clusterHealthOk = true;
                    } else if (healthResponse.getStatus() == ClusterHealthStatus.YELLOW) {
                        logger.info("[PLUGIN] Cluster health is YELLOW for " + INDEX_NAME + ". Index is available for search.");
                        clusterHealthOk = true;
                    } else {
                        logger.warning("[PLUGIN] Cluster health check returned RED status. Index may not be fully available.");
                        validationMessage = "Cluster status is " + healthResponse.getStatus();
                        validationSuccess = false;
                    }
                } catch (Exception ex) {
                    logger.warning("[PLUGIN] Error checking cluster health: " + ex.getMessage());
                    validationMessage = "Error checking cluster health: " + ex.getMessage();
                    validationSuccess = false;
                }

                // בדיקה 3: המתנה עד שהאינדקס מסיים את הסגמנטציה הפנימית
                if (clusterHealthOk) {
                    try {
                        logger.info("[PLUGIN] Forcing segment merges to complete");
                        // מחכה לסיום תהליכי מיזוג הסגמנטים
                        client.admin().indices().prepareForceMerge(INDEX_NAME)
                                .setMaxNumSegments(1)  // ממזג לסגמנט אחד לביצועים אופטימליים
                                .setFlush(true)        // מבצע flush אחרי המיזוג
                                .execute().actionGet();
                        logger.info("[PLUGIN] Force merge completed successfully");
                    } catch (Exception e) {
                        logger.warning("[PLUGIN] Error during force merge: " + e.getMessage());
                    }
                }

                // בדיקה 4: ריענון נוסף לאחר השלמת כל הפעולות
                try {
                    logger.info("[PLUGIN] Performing final index refresh");
                    client.admin().indices().prepareRefresh(INDEX_NAME).execute().actionGet();
                    logger.info("[PLUGIN] Final index refresh completed");
                } catch (Exception e) {
                    logger.warning("[PLUGIN] Error during final refresh: " + e.getMessage());
                }

                // בדיקה 5: וידוא שכל המסמכים שצפויים להיות באינדקס אכן קיימים
                try {
                    logger.info("[PLUGIN] Verifying all documents are searchable");
                    long actualCount = client.prepareSearch(INDEX_NAME)
                            .setSize(0) // אין צורך להחזיר תוצאות, רק ספירה
                            .get()
                            .getHits()
                            .getTotalHits()
                            .value;

                    if (actualCount == chunks.size()) {
                        logger.info("[PLUGIN] All " + chunks.size() + " documents are available in the index");
                    } else {
                        logger.warning("[PLUGIN] Document count mismatch: expected " + chunks.size() +
                                " but found " + actualCount);
                        validationMessage = "Document count mismatch: expected " + chunks.size() +
                                " but found " + actualCount;
                        validationSuccess = false;
                    }

                    // וידוא נוסף - ביצוע חיפוש פשוט כדי לוודא שהאינדקס מגיב
                    try {
                        long searchStart = System.currentTimeMillis();
                        long hitCount = client.prepareSearch(INDEX_NAME)
                                .setQuery(org.elasticsearch.index.query.QueryBuilders.matchAllQuery())
                                .setSize(0)  // אין צורך להחזיר תוצאות, רק ספירה
                                .get()
                                .getHits()
                                .getTotalHits()
                                .value;

                        long searchTime = System.currentTimeMillis() - searchStart;
                        logger.info("[PLUGIN] Search test completed in " + searchTime + "ms with " + hitCount + " hits");

                        if (hitCount != chunks.size()) {
                            logger.warning("[PLUGIN] Search hit count mismatch: expected " + chunks.size() +
                                    " but got " + hitCount);
                        }
                    } catch (Exception e) {
                        logger.warning("[PLUGIN] Error during search test: " + e.getMessage());
                        validationSuccess = false;
                    }

                } catch (Exception ex) {
                    logger.warning("[PLUGIN] Error verifying document count: " + ex.getMessage());
                    validationMessage = "Error verifying document count: " + ex.getMessage();
                    validationSuccess = false;
                }

                double cpuValidationEnd = getProcessCpuLoadPercent();
                double heapValidationEnd = getHeapUsedPercent();
                long validationDuration = System.currentTimeMillis() - validationStart;
                double cpuValidationAvg = (cpuValidationStart + cpuValidationEnd) / 2;
                double heapValidationAvg = (heapValidationStart + heapValidationEnd) / 2;
                logger.info(String.format("[CPU] Step: Validation | Phase: End | Process CPU Load: %.2f%%", cpuValidationEnd));
                logger.info(String.format("[HEAP] Step: Validation | Phase: End | Heap Used: %.2f%%", heapValidationEnd));
                logger.info(String.format("[CPU] Step: Validation | Phase: Avg | Duration: %.2fs | Avg CPU: %.2f%%",
                        validationDuration / 1000.0, cpuValidationAvg));
                logger.info(String.format("[HEAP] Validation | Start: %.2f%% | End: %.2f%% | Avg: %.2f%%",
                        heapValidationStart, heapValidationEnd, heapValidationAvg));

                // ─── Overall metrics end ───────────────────────────────────────────────
                double cpuOverallEnd = getProcessCpuLoadPercent();
                double heapOverallEnd = getHeapUsedPercent();
                long overallDuration = System.currentTimeMillis() - overallStart;
                double cpuOverallAvg = (cpuOverallStart + cpuOverallEnd) / 2;
                double heapOverallAvg = (heapOverallStart + heapOverallEnd) / 2;
                logger.info(String.format("[CPU] Overall | Phase: End | Process CPU Load: %.2f%%", cpuOverallEnd));
                logger.info(String.format("[HEAP] Overall | Phase: End | Heap Used: %.2f%%", heapOverallEnd));
                logger.info(String.format("[CPU] Overall | Phase: Avg | Avg CPU: %.2f%%", cpuOverallAvg));
                logger.info(String.format("[HEAP] Overall | Start: %.2f%% | End: %.2f%% | Avg: %.2f%%",
                        heapOverallStart, heapOverallEnd, heapOverallAvg));

                // ─── Build and send response ────────────────────────────────────────────
                XContentBuilder resp = XContentFactory.jsonBuilder()
                        .startObject()
                        .field("indexing_success", indexedCount == chunks.size())
                        .field("validation_success", validationSuccess)
                        .field("validation_message", validationMessage)
                        .field("requested_chunks", chunks.size())
                        .field("indexed_documents", indexedCount)
                        .field("processing_time_ms", overallDuration)
                        .field("bulk_indexing_method", "parallel")
                        .field("batch_size", BATCH_SIZE)
                        .field("max_concurrent_batches", MAX_CONCURRENT_BATCHES)
                        .field("total_batches", chunks.size() / BATCH_SIZE + (chunks.size() % BATCH_SIZE > 0 ? 1 : 0))
                        .startObject("benchmarks")
                        .field("dotnet_request_ms", requestDotnetDuration)
                        .field("dotnet_request_cpu_percent", cpuDotnetAvg)
                        .field("dotnet_request_heap_percent", heapDotnetAvg)
                        .field("indexing_duration_ms", indexingDuration)
                        .field("indexing_cpu_percent", cpuIndexingAvg)
                        .field("indexing_heap_percent", heapIndexingAvg)
                        .field("refresh_ms", refreshDuration)
                        .field("refresh_cpu_percent", cpuRefreshAvg)
                        .field("refresh_heap_percent", heapRefreshAvg)
                        .field("count_ms", countDuration)
                        .field("count_cpu_percent", cpuCountAvg)
                        .field("count_heap_percent", heapCountAvg)
                        .field("validation_ms", validationDuration)
                        .field("validation_cpu_percent", cpuValidationAvg)
                        .field("validation_heap_percent", heapValidationAvg)
                        .field("overall_cpu_percent", cpuOverallAvg)
                        .field("overall_heap_percent", heapOverallAvg)
                        .endObject()
                        .endObject();

                channel.sendResponse(new RestResponse(RestStatus.OK, resp));
            };
        }

        /**
         * Gets the current CPU load percentage for the JVM process.
         */
        private double getProcessCpuLoadPercent() {
            OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            double load = osBean.getProcessCpuLoad();
            return load >= 0 ? load * 100 : -1;
        }

        /**
         * Gets the current heap memory usage percentage.
         */
        private double getHeapUsedPercent() {
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            MemoryUsage heap = memoryBean.getHeapMemoryUsage();
            return ((double) heap.getUsed() / heap.getMax()) * 100.0;
        }

        private List<Map<String, Object>> requestChunksFromDotnet(String path) {
            logger.info("[PLUGIN] ▼ requestChunksFromDotnet: entry — raw path = \"" + path + "\"");
            try {
                // ────────────────────────────────────────────────────────────────────────────────
                // 1) Sanitize path
                logger.info("[PLUGIN] ▶ Sanitizing path");
                path = path == null ? "" : path.trim();
                if (path.startsWith("\"") && path.endsWith("\"")) {
                    path = path.substring(1, path.length() - 1);
                }
                logger.info("[PLUGIN] ▶ Sanitized path = \"" + path + "\"");

                // ────────────────────────────────────────────────────────────────────────────────
                // 2) Prepare and test connectivity
                URL url = new URL(DOTNET_SERVICE_URL);
                logger.info("[PLUGIN] ▶ Target DOTNET_SERVICE_URL = " + url);
                try (Socket sock = new Socket(url.getHost(), url.getPort())) {
                    logger.info("[PLUGIN] ▶ Raw TCP connect to " + url.getHost() + ":" + url.getPort() + " succeeded");
                } catch (IOException ex) {
                    logger.log(Level.SEVERE, "[PLUGIN] ▶ Raw TCP connect FAILED", ex);
                }

                // ────────────────────────────────────────────────────────────────────────────────
                // 3) Open HTTP connection
                logger.info("[PLUGIN] ▶ Opening HTTP connection");
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setDoOutput(true);
                logger.info("[PLUGIN] ▶ Connection opened (method=POST, Content-Type=application/json)");

                // ────────────────────────────────────────────────────────────────────────────────
                // 4) Build JSON body
                String escapedPath = path.replace("\\", "\\\\").replace("\"", "\\\"");
                String body = "{\"Path\":\"" + escapedPath + "\"}";
                logger.log(
                        Level.INFO,
                        "[PLUGIN] Sending request body: {0}",
                        body
                );

                // ────────────────────────────────────────────────────────────────────────────────
                // 5) Write request body
                logger.info("[PLUGIN] ▶ Writing request body");
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(body.getBytes(StandardCharsets.UTF_8));
                    os.flush();
                    logger.info("[PLUGIN] ▶ Body written and flushed");
                } catch (IOException io) {
                    logger.log(Level.SEVERE, "[PLUGIN] ▶ Failed writing request body", io);
                    readAndLogErrorStream(conn);
                    return null;
                }

                // ────────────────────────────────────────────────────────────────────────────────
                // 6) Read response code
                int code;
                try {
                    code = conn.getResponseCode();
                    logger.info("[PLUGIN] ▶ Response code = " + code);
                } catch (IOException io) {
                    logger.log(Level.SEVERE, "[PLUGIN] ▶ Failed getting response code", io);
                    return null;
                }

                if (code != HttpURLConnection.HTTP_OK) {
                    logger.warning("[PLUGIN] ▶ Non-OK response (" + code + ")");
                    readAndLogErrorStream(conn);
                    return null;
                }

                // ────────────────────────────────────────────────────────────────────────────────
                // 7) Read response body
                StringBuilder resp = new StringBuilder();
                logger.info("[PLUGIN] ▶ Reading response body");


                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    logger.info("[Plugin] starting reading buffer");
                    while ((line = in.readLine()) != null) {
                        resp.append(line);

                    }
                } catch (IOException io) {
                    logger.log(Level.SEVERE, "[PLUGIN] ▶ Failed reading response body", io);
                    return null;
                }

                // ────────────────────────────────────────────────────────────────────────────────
                // 8) Parse JSON into chunks
                logger.info("[PLUGIN] ▶ Parsing chunks JSON");
                return parseChunkList(resp.toString());

            } catch (Exception e) {
                logger.log(Level.SEVERE, "[PLUGIN] ✖ Unexpected exception in requestChunksFromDotnet", e);
                return null;
            }
        }

        /**
         * Helper to read and log the error stream from the connection.
         */
        private void readAndLogErrorStream(HttpURLConnection conn) {
            try (BufferedReader err = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
                String line;
                int index = 0;
                while ((line = err.readLine()) != null) {
                    logger.severe("[PLUGIN] ▶ ErrorStream: " + (++index));
                }
            } catch (Exception ex) {
                logger.log(Level.WARNING, "[PLUGIN] ▶ Failed to read ErrorStream", ex);
            }
        }

        private List<Map<String, Object>> parseChunkList(String json) throws IOException {
            List<Map<String, Object>> chunks = new ArrayList<>();
            logger.info("[PLUGIN] parseChunkList: starting to parse JSON, length=" + (json != null ? json.length() : "null"));

            try (XContentParser parser = XContentType.JSON.xContent()
                    .createParser(
                            NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))
                    )) {
                logger.info("[PLUGIN] parseChunkList: parser initialized");

                XContentParser.Token token = parser.nextToken();
                if (token != XContentParser.Token.START_OBJECT) {
                    logger.warning("[PLUGIN] parseChunkList: expected START_OBJECT but got " + token);
                    throw new IllegalStateException("Expected start of JSON object but got: " + token);
                }

                logger.fine("[PLUGIN] parseChunkList: found END_OBJECT: '" + XContentParser.Token.END_OBJECT + "'");
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    String fieldName = parser.currentName();
                    logger.fine("[PLUGIN] parseChunkList: found field '" + fieldName + "'");
                    token = parser.nextToken();

                    if ("Chunks".equalsIgnoreCase(fieldName)) {
                        if (token != XContentParser.Token.START_ARRAY) {
                            logger.warning("[PLUGIN] parseChunkList: 'Chunks' is not an array, got: " + token);
                            throw new IllegalStateException("'Chunks' should be an array but was: " + token);
                        }
                        logger.info("[PLUGIN] parseChunkList: parsing 'Chunks' array");

                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            Map<String, Object> chunk = parser.map();
                            chunks.add(chunk);
                        }

                        logger.info("[PLUGIN] parseChunkList: finished 'Chunks' array, parsed " + chunks.size() + " chunks so far");
                    } else {
                        logger.fine("[PLUGIN] parseChunkList: skipping children of field '" + fieldName + "'");
                        parser.skipChildren();
                    }
                }

                logger.info("[PLUGIN] parseChunkList: completed parsing JSON, total chunks=" + chunks.size());
            } catch (IOException e) {
                logger.log(Level.SEVERE, "[PLUGIN] IOException in parseChunkList", e);
                throw e;
            } catch (Exception e) {
                logger.log(Level.SEVERE, "[PLUGIN] Unexpected error in parseChunkList", e);
                throw new IOException("Unexpected error parsing JSON chunks", e);
            }

            return chunks;
        }

        /**
         * Prepares an optimized Elasticsearch index for bulk loading.
         * If an index "target_index" exists, it is updated with optimized settings.
         * Otherwise, a new index is created with optimized settings and mapping.
         */
        private void prepareOptimizedIndex(NodeClient client) throws IOException {
            logger.info("[PLUGIN] Preparing optimized index for bulk loading");

            boolean indexExists = Arrays.asList(client.admin().indices().prepareGetIndex().get().getIndices())
                    .contains(INDEX_NAME);

            if (indexExists) {
                // updated settings to existing index.
                Settings settings = Settings.builder()
                        .put("index.refresh_interval", "120s")
                        .put("index.translog.durability", "async")
                        .put("index.translog.flush_threshold_size", "4gb")
                        .put("index.translog.sync_interval", "120s")
                        .build();

                client.admin().indices().prepareUpdateSettings(INDEX_NAME)
                        .setSettings(settings)
                        .get();

                logger.info("[PLUGIN] Updated settings for existing index: " + INDEX_NAME);
            } else {
                // create index with optimal settings
                CreateIndexRequest createRequest = new CreateIndexRequest(INDEX_NAME);
                createRequest.settings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.refresh_interval", "120s")
                        .put("index.translog.durability", "async")
                        .put("index.translog.flush_threshold_size", "4gb")
                        .put("index.translog.sync_interval", "120s")
                        .put("index.merge.scheduler.max_thread_count", 1)
                        .put("index.merge.policy.segments_per_tier", 50)
                        .put("index.merge.policy.max_merged_segment", "5gb")
                        .put("index.indexing.slowlog.threshold.index.warn", "60s")
                        .put("index.indexing.slowlog.threshold.index.info", "30s")
                        .build());

                client.admin().indices().create(createRequest).actionGet();
                logger.info("[PLUGIN] Created optimized index for bulk loading");

                // field mapping
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

                    client.admin().indices().preparePutMapping(INDEX_NAME)
                            .setSource(mappingBuilder)
                            .get();

                    logger.info("[PLUGIN] Created optimized mapping for " + INDEX_NAME);
                } catch (Exception e) {
                    logger.warning("[PLUGIN] Could not set optimized mapping: " + e.getMessage());
                }
            }
        }

        /**
         * Enhanced parallel bulk indexing for improved performance.
         * Divides document chunks into batches and indexes them in parallel.
         */
        private CompletableFuture<Boolean> bulkIndexChunksParallel(List<Map<String, Object>> chunks, NodeClient client) {
            logger.info("[PLUGIN] Starting parallel bulk indexing process for " + chunks.size() + " chunks");
            if (chunks == null || chunks.isEmpty()) {
                CompletableFuture<Boolean> emptyFuture = new CompletableFuture<>();
                emptyFuture.complete(true);
                return emptyFuture;
            }

            ExecutorService executor = Executors.newFixedThreadPool(MAX_CONCURRENT_BATCHES, r -> {
                Thread t = new Thread(r);
                t.setName("bulk-indexer-" + t.getId());
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });

            List<List<Map<String, Object>>> batches = new ArrayList<>();
            for (int i = 0; i < chunks.size(); i += BATCH_SIZE) {
                batches.add(new ArrayList<>(chunks.subList(i, Math.min(i + BATCH_SIZE, chunks.size()))));
            }

            CompletableFuture<Boolean> future = new CompletableFuture<>();
            AtomicInteger completedBatches = new AtomicInteger(0);
            AtomicBoolean hasFailures = new AtomicBoolean(false);
            Semaphore semaphore = new Semaphore(MAX_CONCURRENT_BATCHES);
            int totalBatches = batches.size();

            logger.info("[PLUGIN] Divided into " + totalBatches + " batches, each with up to " +
                    BATCH_SIZE + " chunks, processing up to " + MAX_CONCURRENT_BATCHES + " batches concurrently");

            for (int i = 0; i < batches.size(); i++) {
                final int batchIndex = i;
                final List<Map<String, Object>> batch = batches.get(i);
                CompletableFuture.runAsync(() -> {
                    try {
                        semaphore.acquire();
                        try {
                            processBatch(client, batch, batchIndex, totalBatches);
                        } catch (Exception e) {
                            logger.severe("[PLUGIN] Error processing batch " + (batchIndex + 1) + ": " + e.getMessage());
                            hasFailures.set(true);
                        } finally {
                            semaphore.release();
                            if (completedBatches.incrementAndGet() == totalBatches) {
                                executor.shutdown();
                                future.complete(!hasFailures.get());
                                logger.info("[PLUGIN] All " + totalBatches + " batches completed");
                            }
                        }
                    } catch (InterruptedException e) {
                        logger.severe("[PLUGIN] Batch processing interrupted: " + e.getMessage());
                        hasFailures.set(true);
                        if (completedBatches.incrementAndGet() == totalBatches) {
                            executor.shutdown();
                            future.complete(false);
                        }
                    }
                }, executor);
            }
            return future;
        }

        /**
         * Processes a single batch of document chunks by creating a BulkRequest and sending it to Elasticsearch.
         */
        private void processBatch(NodeClient client, List<Map<String, Object>> batch, int batchIndex, int totalBatches) throws IOException {
            int batchNumber = batchIndex + 1;
            long startTime = System.currentTimeMillis();
            logger.info("[PLUGIN] Processing batch " + batchNumber + "/" + totalBatches + " with " + batch.size() + " chunks");

            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.timeout(org.elasticsearch.core.TimeValue.timeValueMinutes(5));

            for (Map<String, Object> chunk : batch) {
                // Incrementing the document counter as in the original code
                documentCounter.incrementAndGet();
                bulkRequest.add(new IndexRequest(INDEX_NAME).source(chunk, XContentType.JSON));
            }

            CountDownLatch latch = new CountDownLatch(1);
            AtomicBoolean success = new AtomicBoolean(true);

            client.bulk(bulkRequest, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    try {
                        long timeTaken = System.currentTimeMillis() - startTime;
                        if (bulkResponse.hasFailures()) {
                            logger.warning("[PLUGIN] Batch " + batchNumber + "/" + totalBatches +
                                    " completed with failures in " + (timeTaken / 1000.0) + "s: " +
                                    bulkResponse.buildFailureMessage());
                            success.set(false);
                        } else {
                            logger.info("[PLUGIN] Batch " + batchNumber + "/" + totalBatches +
                                    " completed successfully in " + (timeTaken / 1000.0) + "s");
                        }
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        long timeTaken = System.currentTimeMillis() - startTime;
                        logger.severe("[PLUGIN] Batch " + batchNumber + "/" + totalBatches +
                                " failed after " + (timeTaken / 1000.0) + "s: " + e.getMessage());
                        success.set(false);
                    } finally {
                        latch.countDown();
                    }
                }
            });

            try {
                if (!latch.await(10, TimeUnit.MINUTES)) {
                    logger.severe("[PLUGIN] Batch " + batchNumber + "/" + totalBatches + " timed out after 10 minutes");
                    throw new IOException("Batch processing timed out");
                }
                if (!success.get()) {
                    throw new IOException("Batch processing failed");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Batch processing was interrupted", e);
            }
        }
    }