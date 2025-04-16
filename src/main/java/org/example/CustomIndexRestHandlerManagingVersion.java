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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.sun.management.OperatingSystemMXBean;

public class CustomIndexRestHandlerManagingVersion extends BaseRestHandler {

    private static final Logger logger = Logger.getLogger(CustomIndexRestHandlerManagingVersion.class.getName());
    private static final String INDEX_NAME = "target_index";
    private static final String DOTNET_SERVICE_URL = "http://localhost:5203/process/v3";
    private static final String ELASTIC_URL = "http://localhost:9200";
    private static final AtomicInteger documentCounter = new AtomicInteger(0);

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
        // Initiate overall metrics tracking
        long overallStart = System.currentTimeMillis();
        double cpuOverallStart = getProcessCpuLoadPercent();
        double heapOverallStart = getHeapUsedPercent();

        logger.info(String.format("[CPU] Overall | Phase: Start | Process CPU Load: %.2f%%", cpuOverallStart));
        logger.info(String.format("[HEAP] Overall | Phase: Start | Heap Used: %.2f%%", heapOverallStart));

        String json = request.content().utf8ToString();
        String filePath = extractFilePath(json);

        if (filePath == null || filePath.isBlank()) {
            return channel -> {
                try {
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    builder.startObject();
                    builder.field("error", "Missing or invalid 'path' field");
                    builder.endObject();

                    channel.sendResponse(new RestResponse(RestStatus.BAD_REQUEST, builder));
                } catch (IOException e) {
                    channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Error creating response"));
                }
            };
        }

        logger.info("[PLUGIN] Received file processing request for path: " + filePath);

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
        logger.info(String.format("[CPU] Step: Request .NET | Phase: Avg | Duration: %.2fs | Avg CPU: %.2f%%",
                requestDotnetDuration / 1000.0, cpuDotnetAvg));
        logger.info(String.format("[HEAP] Request .NET | Start: %.2f%% | End: %.2f%% | Avg: %.2f%%",
                heapDotnetStart, heapDotnetEnd, heapDotnetAvg));

        logger.info("[PLUGIN] Completed retrieving chunks from .NET service in " + requestDotnetDuration + "ms");

        if (chunks == null || chunks.isEmpty()) {
            return channel -> {
                try {
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    builder.startObject();
                    builder.field("error", "Failed to receive chunks from .NET service");
                    builder.endObject();

                    channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder));
                } catch (IOException e) {
                    channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Error creating response"));
                }
            };
        }

        logger.info("[PLUGIN] Received " + chunks.size() + " chunks from .NET service");

        // STEP 2: Index Chunks
        double cpuIndexingStart = getProcessCpuLoadPercent();
        double heapIndexingStart = getHeapUsedPercent();
        long indexingStart = System.currentTimeMillis();

        logger.info(String.format("[CPU] Step: Indexing | Phase: Start | Process CPU Load: %.2f%%", cpuIndexingStart));
        logger.info(String.format("[HEAP] Step: Indexing | Phase: Start | Heap Used: %.2f%%", heapIndexingStart));

        int i = 1;
        for (Map<String, Object> chunk : chunks) {
            IndexRequest indexRequest = new IndexRequest(INDEX_NAME);
            indexRequest.source(chunk, XContentType.JSON);
            int chunkNumber = i++;

            client.index(indexRequest, ActionListener.wrap(
                    response -> logger.info("[PLUGIN] Successfully indexed chunk #" + chunkNumber + "/" + chunks.size()),
                    exception -> logger.warning("[PLUGIN] Failed to index chunk #" + chunkNumber + ": " + exception.getMessage())
            ));

            documentCounter.incrementAndGet();
        }

        double cpuIndexingEnd = getProcessCpuLoadPercent();
        double heapIndexingEnd = getHeapUsedPercent();
        long indexingDuration = System.currentTimeMillis() - indexingStart;
        double cpuIndexingAvg = (cpuIndexingStart + cpuIndexingEnd) / 2;
        double heapIndexingAvg = (heapIndexingStart + heapIndexingEnd) / 2;

        logger.info(String.format("[CPU] Step: Indexing | Phase: End | Process CPU Load: %.2f%%", cpuIndexingEnd));
        logger.info(String.format("[HEAP] Step: Indexing | Phase: End | Heap Used: %.2f%%", heapIndexingEnd));
        logger.info(String.format("[CPU] Step: Indexing | Phase: Avg | Duration: %.2fs | Avg CPU: %.2f%%",
                indexingDuration / 1000.0, cpuIndexingAvg));
        logger.info(String.format("[HEAP] Indexing | Start: %.2f%% | End: %.2f%% | Avg: %.2f%%",
                heapIndexingStart, heapIndexingEnd, heapIndexingAvg));

        logger.info("[PLUGIN] Indexing stage completed in " + indexingDuration + "ms");

        // STEP 3: Refresh Index
        double cpuRefreshStart = getProcessCpuLoadPercent();
        double heapRefreshStart = getHeapUsedPercent();
        long refreshStart = System.currentTimeMillis();

        logger.info(String.format("[CPU] Step: Refresh | Phase: Start | Process CPU Load: %.2f%%", cpuRefreshStart));
        logger.info(String.format("[HEAP] Step: Refresh | Phase: Start | Heap Used: %.2f%%", heapRefreshStart));

        refreshIndex();

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

        logger.info("[PLUGIN] Index refresh completed in " + refreshDuration + "ms");

        // STEP 4: Count Documents
        double cpuCountStart = getProcessCpuLoadPercent();
        double heapCountStart = getHeapUsedPercent();
        long countStart = System.currentTimeMillis();

        logger.info(String.format("[CPU] Step: Count | Phase: Start | Process CPU Load: %.2f%%", cpuCountStart));
        logger.info(String.format("[HEAP] Step: Count | Phase: Start | Heap Used: %.2f%%", heapCountStart));

        int indexedCount = countDocuments();

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

        logger.info("[PLUGIN] Document count verified: " + indexedCount + ". Duration: " + countDuration + "ms");

        // Overall metrics calculation
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

        logger.info("[PLUGIN] End-to-end processing completed in " + overallDuration + "ms");

        return channel -> {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("indexing_success", indexedCount == chunks.size());
            builder.field("requested_chunks", chunks.size());
            builder.field("indexed_documents", indexedCount);
            builder.field("processing_time_ms", overallDuration);
            builder.field("indexing_duration_ms", indexingDuration);
            builder.field("dotnet_request_ms", requestDotnetDuration);
            builder.field("index_refresh_ms", refreshDuration);
            builder.field("count_documents_ms", countDuration);

            // Add CPU and Heap benchmarks to response
            builder.startObject("benchmarks");
            builder.field("dotnet_request_cpu_percent", cpuDotnetAvg);
            builder.field("dotnet_request_heap_percent", heapDotnetAvg);
            builder.field("indexing_cpu_percent", cpuIndexingAvg);
            builder.field("indexing_heap_percent", heapIndexingAvg);
            builder.field("refresh_cpu_percent", cpuRefreshAvg);
            builder.field("refresh_heap_percent", heapRefreshAvg);
            builder.field("count_cpu_percent", cpuCountAvg);
            builder.field("count_heap_percent", heapCountAvg);
            builder.field("overall_cpu_percent", cpuOverallAvg);
            builder.field("overall_heap_percent", heapOverallAvg);
            builder.endObject();

            builder.endObject();

            channel.sendResponse(new RestResponse(RestStatus.OK, builder));
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

    private String extractFilePath(String json) {
        try {
            int start = json.indexOf(":");
            int end = json.lastIndexOf("}");
            if (start == -1 || end == -1) return null;
            return json.substring(start + 2, end - 1);
        } catch (Exception e) {
            logger.warning("Failed to extract path: " + e.getMessage());
            return null;
        }
    }

    private List<Map<String, Object>> requestChunksFromDotnet(String path) {
        try {
            URL url = new URL(DOTNET_SERVICE_URL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            String body = "{\"path\": \"" + path + "\"}";
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }

            if (conn.getResponseCode() != 200) {
                logger.warning("[PLUGIN] .NET service responded with code: " + conn.getResponseCode());
                return null;
            }

            StringBuilder response = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
            }

            return parseChunkList(response.toString());
        } catch (Exception e) {
            logger.warning("[PLUGIN] Failed to request chunks from .NET: " + e.getMessage());
            return null;
        }
    }

    private List<Map<String, Object>> parseChunkList(String json) throws IOException {
        List<Map<String, Object>> chunks = new ArrayList<>();

        try (XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)))) {

            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalStateException("Expected start of JSON object");
            }

            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();

                if ("chunks".equals(fieldName)) {
                    if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                        throw new IllegalStateException("'chunks' should be an array");
                    }

                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        Map<String, Object> chunk = parser.map();
                        chunks.add(chunk);
                    }
                } else {
                    parser.skipChildren();
                }
            }
        }

        return chunks;
    }

    private void refreshIndex() {
        try {
            URL url = new URL(ELASTIC_URL + "/" + INDEX_NAME + "/_refresh");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.getResponseCode();
        } catch (Exception e) {
            logger.warning("Failed to refresh index: " + e.getMessage());
        }
    }

    private int countDocuments() {
        try {
            URL url = new URL(ELASTIC_URL + "/" + INDEX_NAME + "/_count");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line = in.readLine();
            in.close();

            int idx = line.indexOf(":");
            int end = line.indexOf("}");
            return Integer.parseInt(line.substring(idx + 1, end).trim());
        } catch (Exception e) {
            logger.warning("Failed to count documents: " + e.getMessage());
            return -1;
        }
    }
}