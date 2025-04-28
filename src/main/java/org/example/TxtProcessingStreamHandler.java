package org.example;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

import com.sun.management.OperatingSystemMXBean;

public class TxtProcessingStreamHandler extends BaseRestHandler {

    // Logger instance for diagnostic messages
    private static final Logger logger = Logger.getLogger(TxtProcessingStreamHandler.class.getName());

    // Size of each read chunk from the file (4 MB)
    private static final int CHUNK_SIZE_BYTES = 4 * 1024 * 1024;
    // Number of documents per bulk request to Elasticsearch
    private static final int BULK_SIZE = 50;
    // Maximum number of concurrent bulk requests
    private static final int MAX_CONCURRENT_BULKS = 5;
    // Capacity of the queue to hold pending batches (backpressure)
    private static final int QUEUE_CAPACITY = MAX_CONCURRENT_BULKS * 2;

    @Override
    public String getName() {
        // Identifier for this REST handler
        return "txt_processing_optimized";
    }

    @Override
    public List<Route> routes() {
        // Define HTTP route for this handler
        return List.of(new Route(RestRequest.Method.POST, "/_process_txt_optimized"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Parse JSON request body into a map
        Map<String, Object> params = request.contentParser().map();
        // Extract 'path' parameter pointing to the input file
        String path = (String) params.get("path");
        if (path == null) {
            // Reject request if 'path' is missing
            throw new IllegalArgumentException("Missing 'path' parameter");
        }
        // Return a consumer that invokes processStream when called
        return channel -> processStream(path, client, channel);
    }

    private void processStream(String path, NodeClient client, RestChannel channel) {
        // Record start time and initial CPU/heap usage
        long startTime = System.currentTimeMillis();
        double startCpu = getCpuLoad();
        double startHeap = getHeapUsage();
        logger.info(String.format("[START] CPU: %.2f%%, Heap: %.2f%%", startCpu, startHeap));

        try {
            // Validate input file exists
            File file = new File(path);
            if (!file.exists() || !file.isFile()) {
                throw new FileNotFoundException("File not found: " + path);
            }

            // Delete existing index and create a fresh one
            recreateIndex(client);

            // Create a blocking queue to hold batches, enforcing backpressure
            BlockingQueue<List<Map<String, Object>>> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
            // Thread pool to dispatch bulk requests concurrently
            ExecutorService bulkExecutor = Executors.newFixedThreadPool(MAX_CONCURRENT_BULKS);
            // Track consumer tasks for later waiting
            List<CompletableFuture<Void>> consumers = new ArrayList<>();

            // Launch consumer threads: each takes batches and sends them
            for (int i = 0; i < MAX_CONCURRENT_BULKS; i++) {
                CompletableFuture<Void> consumer = CompletableFuture.runAsync(() -> {
                    try {
                        while (true) {
                            // Wait for next batch from queue
                            List<Map<String, Object>> batch = queue.take();
                            // Empty batch signals termination (poison pill)
                            if (batch.isEmpty()) {
                                break;
                            }
                            // Send batch to Elasticsearch and wait for completion
                            bulkIndex(client, batch).join();
                        }
                    } catch (InterruptedException e) {
                        // Restore interrupt status
                        Thread.currentThread().interrupt();
                    }
                }, bulkExecutor);
                // Keep track of each consumer future
                consumers.add(consumer);
            }

            // Producer: read the file in chunks, split into lines, and enqueue batches
            try (RandomAccessFile raf = new RandomAccessFile(file, "r");
                 FileChannel reader = raf.getChannel()) {

                // Allocate a direct ByteBuffer for file reads (off-heap)
                ByteBuffer buffer = ByteBuffer.allocateDirect(CHUNK_SIZE_BYTES);
                // StringBuilder to hold partial line between chunks
                StringBuilder leftover = new StringBuilder();
                // List to accumulate a batch of documents
                List<Map<String, Object>> batch = new ArrayList<>(BULK_SIZE);
                int sequenceNum = 0;

                // Read until EOF
                while (reader.read(buffer) > 0) {
                    buffer.flip();
                    // Transfer bytes from buffer into heap byte[]
                    byte[] data = new byte[buffer.limit()];
                    buffer.get(data);
                    buffer.clear();

                    // Append decoded text to leftover for overlapping lines
                    leftover.append(new String(data, StandardCharsets.UTF_8));
                    // Split into lines
                    String[] lines = leftover.toString().split("\\r?\\n");
                    leftover.setLength(0);
                    // If last line is incomplete, save it for the next chunk
                    if (!leftover.toString().endsWith("\n")) {
                        leftover.append(lines[lines.length - 1]);
                    }
                    // Determine how many complete lines we have
                    int limit = leftover.length() > 0 ? lines.length - 1 : lines.length;
                    // Process each complete line
                    for (int i = 0; i < limit; i++) {
                        // Build document map
                        Map<String, Object> doc = new HashMap<>();
                        doc.put("content", lines[i]);
                        doc.put("fileName", file.getName());
                        doc.put("sequence", sequenceNum++);
                        batch.add(doc);
                        // Once batch size reached, enqueue it
                        if (batch.size() >= BULK_SIZE) {
                            queue.put(new ArrayList<>(batch));
                            logger.info("Enqueued bulk of size " + batch.size());
                            batch.clear();
                        }
                    }
                }
                // Enqueue any remaining documents in the final batch
                if (!batch.isEmpty()) {
                    queue.put(new ArrayList<>(batch));
                    logger.info("Enqueued final bulk of size " + batch.size());
                }
            } finally {
                // Send empty batches (poison pills) to signal consumers to finish
                for (int i = 0; i < MAX_CONCURRENT_BULKS; i++) {
                    queue.put(Collections.emptyList());
                }
            }

            // Wait for all consumer threads to complete their work
            CompletableFuture.allOf(consumers.toArray(new CompletableFuture[0])).join();
            // Shut down the bulk execution pool
            bulkExecutor.shutdown();

            // Refresh the Elasticsearch index to make documents searchable
            client.admin().indices().prepareRefresh("target_index").get();
            logger.info("Index refresh complete.");

            // Calculate final metrics and log them
            long duration = System.currentTimeMillis() - startTime;
            double endCpu = getCpuLoad();
            double endHeap = getHeapUsage();
            logger.info(String.format("[END] Duration: %.2fs, CPU: %.2f%%->%.2f%%, Heap: %.2f%%->%.2f%%",
                    duration / 1000.0, startCpu, endCpu, startHeap, endHeap));

            // Build and send success response
            XContentBuilder resp = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("success", true)
                    .field("processingTimeSec", duration / 1000.0)
                    .field("cpuLoadStart", startCpu)
                    .field("cpuLoadEnd", endCpu)
                    .field("heapUsageStart", startHeap)
                    .field("heapUsageEnd", endHeap)
                    .endObject();
            channel.sendResponse(new RestResponse(RestStatus.OK, resp));

        } catch (Exception e) {
            // Log failure and return error response
            logger.severe("Processing failed: " + e.getMessage());
            try {
                XContentBuilder resp = XContentFactory.jsonBuilder()
                        .startObject()
                        .field("success", false)
                        .field("error", e.getMessage())
                        .endObject();
                channel.sendResponse(new RestResponse(RestStatus.INTERNAL_SERVER_ERROR, resp));
            } catch (IOException io) {
                logger.severe("Failed to send error response: " + io.getMessage());
            }
        }
    }

    /**
     * Send a bulk request to Elasticsearch asynchronously, completing the future when done.
     */
    private CompletableFuture<Void> bulkIndex(NodeClient client, List<Map<String, Object>> docs) {
        BulkRequest req = new BulkRequest();
        for (Map<String, Object> doc : docs) {
            req.add(new IndexRequest("target_index").source(doc, XContentType.JSON));
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        client.bulk(req, ActionListener.wrap(
                resp -> {
                    if (resp.hasFailures()) {
                        logger.warning("Bulk failures: " + resp.buildFailureMessage());
                    }
                    future.complete(null);
                },
                ex -> future.completeExceptionally(ex)
        ));
        return future;
    }

    /**
     * Delete and recreate the target_index with optimized settings.
     */
    private void recreateIndex(NodeClient client) throws IOException {
        try {
            client.admin().indices().delete(new DeleteIndexRequest("target_index")).actionGet();
        } catch (Exception ignored) {}
        client.admin().indices().prepareCreate("target_index")
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.refresh_interval", "120s"))
                .get();
        logger.info("Index 'target_index' created.");
    }

    /**
     * Get current process CPU load percentage.
     */
    private double getCpuLoad() {
        OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        return os.getProcessCpuLoad() * 100;
    }

    /**
     * Get current heap usage percentage.
     */
    private double getHeapUsage() {
        MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = mem.getHeapMemoryUsage();
        return (double) heap.getUsed() / heap.getMax() * 100;
    }
}