package org.example;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.DeprecationHandler;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class CustomIndexRestHandlerManagingVersion extends BaseRestHandler {

    private static final Logger logger = Logger.getLogger(CustomIndexRestHandlerManagingVersion.class.getName());
    private static final String INDEX_NAME = "target_index";
    private static final String DOTNET_SERVICE_URL = "http://localhost:5203/process/v3";
    private static final String ELASTIC_URL = "http://localhost:9200";
    private static final AtomicInteger documentCounter = new AtomicInteger(0);
    private static long startTime;

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
        startTime = System.currentTimeMillis();
        String json = request.content().utf8ToString();
        String filePath = extractFilePath(json);

        if (filePath == null || filePath.isBlank()) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "Missing or invalid 'path' field"));
        }

        logger.info("[PLUGIN] Received file processing request for: " + filePath);
        List<Map<String, Object>> chunks = requestChunksFromDotnet(filePath);

        if (chunks == null || chunks.isEmpty()) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, "Failed to receive chunks from .NET service"));
        }

        logger.info("[PLUGIN] Received " + chunks.size() + " chunks from .NET service");
        long indexingStart = System.currentTimeMillis();

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

        long indexingTime = System.currentTimeMillis() - indexingStart;
        refreshIndex();
        int indexedCount = countDocuments();

        long totalTime = System.currentTimeMillis() - startTime;
        logger.info("[PLUGIN] Indexing completed: " + indexedCount + " documents in " + totalTime + " ms");

        return channel -> {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("indexing_success", indexedCount == chunks.size());
            builder.field("requested_chunks", chunks.size());
            builder.field("indexed_documents", indexedCount);
            builder.field("processing_time_ms", totalTime);
            builder.field("indexing_duration_ms", indexingTime);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
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
