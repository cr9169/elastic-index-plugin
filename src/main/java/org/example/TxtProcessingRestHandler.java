package org.example;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TxtProcessingRestHandler processes large TXT files by splitting them into chunks,
 * preparing an optimized Elasticsearch index, and bulk indexing the chunks.
 * <p>
 * This REST handler is exposed as a POST endpoint at {@code /_process_txt}.
 * It expects a JSON payload with a "path" field pointing to a TXT file.
 * The file is validated, split into chunks, and then indexed in Elasticsearch.
 */
public class TxtProcessingRestHandler extends BaseRestHandler {

    // Constants (can be replaced with external configuration)
    private static final int DEFAULT_CHUNK_SIZE_BYTES = 9 * 1024 * 1024; // 9 MB
    private static final int MAX_PARALLELISM = 4;
    private static final int MAX_CONCURRENT_BATCHES = 5; // Increased from 3 to 5
    private static final int BATCH_SIZE = 30; // Increased from 20 to 30 chunks per batch

    // Allowed directory for processing files
    private static final String ALLOWED_DIRECTORY = "C:/Users/BarGabay/big files";
    private static final Logger logger = Logger.getLogger(TxtProcessingRestHandler.class.getName());

    /**
     * Returns the unique name of this REST handler.
     *
     * @return the name "txt_processing_rest_handler".
     */
    @Override
    public String getName() {
        return "txt_processing_rest_handler";
    }

    /**
     * Defines the REST routes handled by this handler.
     *
     * @return a list containing one route: POST /_process_txt.
     */
    @Override
    public List<Route> routes() {
        return Collections.singletonList(
                new Route(RestRequest.Method.POST, "/_process_txt")
        );
    }

    /**
     * Prepares the REST request by parsing the JSON payload for the file path,
     * validating the file's location, processing the file, and sending a JSON response.
     *
     * @param request the incoming REST request.
     * @param client  the NodeClient used to execute Elasticsearch operations.
     * @return a RestChannelConsumer that sends the response.
     * @throws IOException if an I/O error occurs during request processing.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try {
            // Parse JSON content; expect a "path" field.
            Map<String, Object> sourceAsMap = request.contentParser().map();
            String filePath = (String) sourceAsMap.get("path");
            if (filePath == null) {
                throw new IllegalArgumentException("Missing 'path' parameter");
            }

            // Validate that the requested file is within the allowed directory.
            File requestedFile = new File(filePath);
            File allowedDir = new File(ALLOWED_DIRECTORY);
            if (!requestedFile.getCanonicalPath().startsWith(allowedDir.getCanonicalPath())) {
                throw new SecurityException("Access denied: File is outside allowed directory");
            }

            // Process the file.
            return channel -> {
                try {
                    ProcessingResponse response = processFile(filePath, client);

                    // Build JSON response.
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    builder.startObject();
                    builder.field("success", response.isSuccess());
                    builder.field("errorMessage", response.getErrorMessage());
                    builder.field("chunkCount", response.getChunkCount());
                    builder.field("processingTimeInSeconds", response.getProcessingTimeInSeconds());
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error processing file", e);
                    sendErrorResponse(channel, e);
                }
            };
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error preparing request", e);
            return channel -> sendErrorResponse(channel, e);
        }
    }

    /**
     * Sends an error response in JSON format.
     *
     * @param channel the REST channel to send the response.
     * @param e       the exception that occurred.
     * @throws IOException if an I/O error occurs while sending the response.
     */
    private void sendErrorResponse(RestChannel channel, Exception e) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("success", false);
        builder.field("errorMessage", "Error processing request: " + e.getMessage());
        builder.endObject();
        channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder));
    }

    /**
     * Prepares an optimized Elasticsearch index for bulk loading.
     * <p>
     * This method attempts to delete an existing index "target_index" and then creates a new index with
     * settings optimized for high write performance. It also attempts to set an optimized mapping.
     *
     * @param client the NodeClient used to perform index operations.
     * @throws IOException if an I/O error occurs during index creation.
     */
    private void prepareOptimizedIndex(NodeClient client) throws IOException {
        logger.info("Preparing optimized index for bulk loading");

        // Attempt to delete the existing index.
        try {
            client.admin().indices().delete(new DeleteIndexRequest("target_index")).actionGet();
            logger.info("Deleted existing target_index");
        } catch (Exception e) {
            logger.info("No existing index to delete: " + e.getMessage());
        }

        // Create a new index with optimized settings.
        CreateIndexRequest createRequest = new CreateIndexRequest("target_index");
        createRequest.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)  // No replicas during indexing
                .put("index.refresh_interval", "120s")  // Increased refresh interval
                .put("index.translog.durability", "async")  // Asynchronous translog durability
                .put("index.translog.flush_threshold_size", "4gb")  // Increased flush threshold
                .put("index.translog.sync_interval", "120s")  // Increased sync interval
                .put("index.merge.scheduler.max_thread_count", 1) // Limit merge threads
                .put("index.merge.policy.segments_per_tier", 50)  // Increase allowed segments
                .put("index.merge.policy.max_merged_segment", "5gb") // Increase max merged segment size
                .put("index.indexing.slowlog.threshold.index.warn", "60s") // Warning threshold
                .put("index.indexing.slowlog.threshold.index.info", "30s") // Info threshold
                .build());

        client.admin().indices().create(createRequest).actionGet();
        logger.info("Created optimized index for bulk loading");

        try {
            // Attempt to set an optimized mapping for the index.
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

            client.admin().indices().preparePutMapping("target_index")
                    .setSource(mappingBuilder)
                    .get();

            logger.info("Created optimized mapping for target_index");
        } catch (Exception e) {
            logger.warning("Could not set optimized mapping: " + e.getMessage());
        }
    }

    /**
     * Processes a TXT file by verifying its properties, splitting it into chunks,
     * preparing an optimized index, and bulk indexing the chunks.
     *
     * @param filePath the path to the TXT file.
     * @param client   the NodeClient used for Elasticsearch operations.
     * @return a ProcessingResponse containing the outcome and metrics of the process.
     */
    private ProcessingResponse processFile(String filePath, NodeClient client) {
        ProcessingResponse response = new ProcessingResponse();
        response.setId(UUID.randomUUID().toString());
        response.setFilePath(filePath);
        response.setSuccess(false);
        response.setBenchmarks(new HashMap<>());
        long overallStart = System.currentTimeMillis();

        try {
            File file = new File(filePath);
            if (!file.exists()) {
                response.setErrorMessage("File does not exist: " + filePath);
                return response;
            }
            if (!filePath.toLowerCase().endsWith(".txt")) {
                response.setErrorMessage("Invalid file extension. Expected .txt");
                return response;
            }

            long fileSize = file.length();
            response.setFileSizeInBytes(fileSize);
            String fileId = file.getName().replace(" ", "_") + "_" + fileSize + "_" + file.lastModified();

            // Use default chunk size.
            int chunkSizeInBytes = DEFAULT_CHUNK_SIZE_BYTES;

            // Process the file into chunks using the optimized chunking method.
            List<DocumentChunk> chunks = chunkTextFileOptimized(filePath, fileId, chunkSizeInBytes);
            response.setChunkCount(chunks.size());

            // Log processed characters for verification.
            long totalProcessedChars = chunks.stream().mapToLong(c -> c.getContent().length()).sum();
            logger.info("Total processed characters: " + totalProcessedChars + ", Original file size (bytes): " + fileSize);

            // Prepare the optimized index.
            try {
                prepareOptimizedIndex(client);
            } catch (Exception e) {
                logger.warning("Could not optimize index settings: " + e.getMessage());
            }

            // Bulk index the chunks concurrently.
            CompletableFuture<Boolean> bulkFuture = bulkIndexChunksParallel(chunks, client);

            // Wait for bulk indexing to complete.
            boolean bulkResult = bulkFuture.get();
            if (!bulkResult) {
                response.setErrorMessage("Bulk indexing failed");
                return response;
            }

            response.setSuccess(true);
            long overallTime = System.currentTimeMillis() - overallStart;
            response.setProcessingTimeInSeconds(overallTime / 1000.0);

            logger.info("File processing and indexing completed for " + filePath +
                    ". Total processing time: " + (overallTime / 1000.0) + " seconds" +
                    ". Total chunks: " + response.getChunkCount() +
                    ". File size: " + response.getFileSizeInBytes() + " bytes");

            return response;
        } catch (InterruptedException | ExecutionException ex) {
            logger.log(Level.SEVERE, "Error processing TXT file", ex);
            response.setErrorMessage("Error processing TXT: " + ex.getMessage());
            return response;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Error processing TXT file", ex);
            response.setErrorMessage("Error processing TXT: " + ex.getMessage());
            return response;
        }
    }

    /**
     * Optimized method to split a TXT file into chunks by reading it sequentially using a direct ByteBuffer.
     *
     * @param filePath         the path to the TXT file.
     * @param fileId           a unique identifier for the file.
     * @param chunkSizeInBytes the size of each chunk in bytes.
     * @return a list of DocumentChunk objects representing the file chunks.
     * @throws IOException if an I/O error occurs during file reading.
     */
    private List<DocumentChunk> chunkTextFileOptimized(String filePath, String fileId, int chunkSizeInBytes) throws IOException {
        File file = new File(filePath);
        long fileSize = file.length();
        int chunkCount = (int) Math.ceil((double) fileSize / chunkSizeInBytes);
        List<DocumentChunk> chunks = new ArrayList<>(chunkCount);

        logger.info("Starting optimized file chunking for " + filePath + " into " + chunkCount + " chunks");
        long startTime = System.currentTimeMillis();

        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             FileChannel channel = raf.getChannel()) {

            ByteBuffer buffer = ByteBuffer.allocateDirect(chunkSizeInBytes);

            for (int i = 0; i < chunkCount; i++) {
                buffer.clear();
                long startPos = (long) i * chunkSizeInBytes;
                channel.position(startPos);

                int bytesRead = channel.read(buffer);
                buffer.flip();

                byte[] bytes = new byte[bytesRead];
                buffer.get(bytes);

                String rawContent = safeUtf8Decode(bytes);
                boolean isFirst = i == 0;
                boolean isLast = i == chunkCount - 1;
                String content = adjustChunkBoundaries(rawContent, !isFirst, !isLast);

                DocumentChunk chunk = new DocumentChunk();
                chunk.setId(UUID.randomUUID().toString());
                chunk.setOriginalFilePath(filePath);
                chunk.setFileName(file.getName());
                chunk.setFileIdentifier(fileId);
                chunk.setSequenceNumber(i + 1);
                chunk.setStartPage(1);
                chunk.setEndPage(1);
                chunk.setTotalPages(1);
                chunk.setContent(content);
                chunk.setProcessedAt(new Date());
                chunk.setFileSizeInBytes(fileSize);
                chunk.setTotalChunks(chunkCount);
                chunks.add(chunk);
            }

            long totalTime = System.currentTimeMillis() - startTime;
            logger.info("Completed file chunking in " + (totalTime / 1000.0) + " seconds");

            return chunks;
        }
    }

    /**
     * Legacy method for splitting a TXT file into chunks using memory mapping.
     * Retained for backward compatibility.
     *
     * @param filePath         the path to the TXT file.
     * @param fileId           a unique identifier for the file.
     * @param chunkSizeInBytes the size of each chunk in bytes.
     * @return a list of DocumentChunk objects representing the file chunks.
     * @throws IOException          if an I/O error occurs during file reading.
     * @throws InterruptedException if the thread is interrupted.
     * @throws ExecutionException   if an error occurs during asynchronous processing.
     */
    private List<DocumentChunk> chunkTextFileParallel(String filePath, String fileId, int chunkSizeInBytes)
            throws IOException, InterruptedException, ExecutionException {
        File file = new File(filePath);
        long fileSize = file.length();
        int chunkCount = (int) Math.ceil((double) fileSize / chunkSizeInBytes);
        List<Future<DocumentChunk>> futures = new ArrayList<>();

        // Custom ThreadFactory for clear thread naming.
        ThreadFactory threadFactory = new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = defaultFactory.newThread(r);
                thread.setName("txt-chunk-processor-" + thread.getId());
                return thread;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(
                Math.min(MAX_PARALLELISM, chunkCount), threadFactory);

        RandomAccessFile raf = null;
        FileChannel channel = null;

        try {
            raf = new RandomAccessFile(file, "r");
            channel = raf.getChannel();
            final FileChannel finalChannel = channel;

            for (int i = 0; i < chunkCount; i++) {
                final int index = i;
                final long startPos = (long) index * chunkSizeInBytes;
                final long size = Math.min(chunkSizeInBytes, fileSize - startPos);

                futures.add(executor.submit(() -> {
                    MappedByteBuffer buffer = null;
                    try {
                        // Synchronize mapping to avoid concurrent issues.
                        synchronized (finalChannel) {
                            buffer = finalChannel.map(FileChannel.MapMode.READ_ONLY, startPos, size);
                        }

                        byte[] bytes = new byte[(int) size];
                        buffer.get(bytes);

                        String rawContent = safeUtf8Decode(bytes);
                        boolean isFirst = index == 0;
                        boolean isLast = index == chunkCount - 1;
                        String content = adjustChunkBoundaries(rawContent, !isFirst, !isLast);

                        // Clean the mapped buffer.
                        cleanMappedByteBuffer(buffer);

                        DocumentChunk chunk = new DocumentChunk();
                        chunk.setId(UUID.randomUUID().toString());
                        chunk.setOriginalFilePath(filePath);
                        chunk.setFileName(file.getName());
                        chunk.setFileIdentifier(fileId);
                        chunk.setSequenceNumber(index + 1);
                        chunk.setStartPage(1);
                        chunk.setEndPage(1);
                        chunk.setTotalPages(1);
                        chunk.setContent(content);
                        chunk.setProcessedAt(new Date());
                        chunk.setFileSizeInBytes(fileSize);
                        chunk.setTotalChunks(chunkCount);
                        return chunk;
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, "Error processing chunk " + index, e);
                        throw e;
                    }
                }));
            }

            executor.shutdown();
            boolean completed = executor.awaitTermination(1, TimeUnit.HOURS);
            if (!completed) {
                logger.log(Level.SEVERE, "Processing chunks timed out after 1 hour");
            }

            List<DocumentChunk> chunks = new ArrayList<>();
            for (Future<DocumentChunk> future : futures) {
                chunks.add(future.get());
            }
            chunks.sort(Comparator.comparingInt(DocumentChunk::getSequenceNumber));
            return chunks;

        } finally {
            if (executor != null && !executor.isTerminated()) {
                try {
                    logger.info("Shutting down executor service");
                    executor.shutdownNow();
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Error shutting down executor", e);
                }
            }
            if (channel != null) {
                try {
                    logger.info("Closing file channel");
                    channel.close();
                } catch (IOException e) {
                    logger.log(Level.WARNING, "Error closing file channel", e);
                }
            }
            if (raf != null) {
                try {
                    logger.info("Closing random access file");
                    raf.close();
                } catch (IOException e) {
                    logger.log(Level.WARNING, "Error closing random access file", e);
                }
            }
        }
    }

    /**
     * Safely decodes a byte array into a UTF-8 string, ensuring the last byte does not split a multi-byte character.
     *
     * @param bytes the byte array to decode.
     * @return the decoded UTF-8 string.
     */
    private String safeUtf8Decode(byte[] bytes) {
        int len = bytes.length;
        // Check if the last byte is a continuation byte (0x80-0xBF)
        while (len > 0 && (bytes[len - 1] & 0xC0) == 0x80) {
            len--;
        }
        return new String(bytes, 0, len, StandardCharsets.UTF_8);
    }

    /**
     * Adjusts the boundaries of a chunk's content by finding appropriate break points,
     * ensuring that chunks do not cut off in the middle of words or sentences.
     *
     * @param content     the raw chunk content.
     * @param adjustStart if true, adjust the start boundary.
     * @param adjustEnd   if true, adjust the end boundary.
     * @return the adjusted content.
     */
    private String adjustChunkBoundaries(String content, boolean adjustStart, boolean adjustEnd) {
        if (content == null || content.isEmpty()) {
            return "";
        }

        int startIdx = 0;
        int endIdx = content.length();

        if (adjustStart) {
            startIdx = findFirstBreakPoint(content);
        }
        if (adjustEnd) {
            endIdx = findLastBreakPoint(content);
        }

        if (startIdx >= endIdx || startIdx >= content.length()) {
            return content;
        }

        return content.substring(startIdx, endIdx);
    }

    /**
     * Finds the first break point in the text within the first 500 characters.
     *
     * @param text the text to search.
     * @return the index after the first break point, or 0 if none is found.
     */
    private int findFirstBreakPoint(String text) {
        if (text == null || text.isEmpty()) {
            return 0;
        }

        int limit = Math.min(text.length(), 500);
        for (int i = 0; i < limit; i++) {
            if (text.charAt(i) == '\n' || Character.isWhitespace(text.charAt(i))) {
                return i + 1;
            }
        }
        return 0;
    }

    /**
     * Finds the last break point in the text within the last 500 characters.
     *
     * @param text the text to search.
     * @return the index after the last break point, or the text length if none is found.
     */
    private int findLastBreakPoint(String text) {
        if (text == null || text.isEmpty()) {
            return 0;
        }

        int start = Math.max(0, text.length() - 500);
        for (int i = text.length() - 1; i >= start; i--) {
            if (text.charAt(i) == '\n' ||
                    ((text.charAt(i) == '.' || text.charAt(i) == '!' || text.charAt(i) == '?') &&
                            (i + 1 >= text.length() || Character.isWhitespace(text.charAt(i + 1))))) {
                return i + 1;
            }
        }
        return text.length();
    }

    /**
     * Cleans up a MappedByteBuffer using reflection or alternative methods to release its resources.
     *
     * @param buffer the MappedByteBuffer to clean.
     */
    private void cleanMappedByteBuffer(final MappedByteBuffer buffer) {
        if (buffer == null) {
            return;
        }

        try {
            try {
                Method cleanerMethod = buffer.getClass().getMethod("cleaner");
                cleanerMethod.setAccessible(true);
                Object cleaner = cleanerMethod.invoke(buffer);
                if (cleaner != null) {
                    Method cleanMethod = cleaner.getClass().getMethod("clean");
                    cleanMethod.setAccessible(true);
                    cleanMethod.invoke(cleaner);
                    return;
                }
            } catch (Exception ignored) {
            }

            try {
                Method getCleanerMethod = buffer.getClass().getDeclaredMethod("cleaner");
                getCleanerMethod.setAccessible(true);
                Object cleaner = getCleanerMethod.invoke(buffer);
                Method cleanMethod = cleaner.getClass().getDeclaredMethod("clean");
                cleanMethod.setAccessible(true);
                cleanMethod.invoke(cleaner);
                return;
            } catch (Exception ignored) {
            }

            try {
                Field unsafeField = Class.forName("sun.misc.Unsafe").getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                Object unsafe = unsafeField.get(null);
                Method invokeCleaner = unsafe.getClass().getMethod("invokeCleaner", ByteBuffer.class);
                invokeCleaner.invoke(unsafe, buffer);
                return;
            } catch (Exception ignored) {
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to clean direct buffer", e);
        }

        buffer.clear();
        System.gc();
    }

    /**
     * Enhanced version of bulkIndexChunksParallel for improved performance.
     * <p>
     * This method divides document chunks into batches and indexes them in parallel using an ExecutorService.
     *
     * @param chunks the list of document chunks to index.
     * @param client the NodeClient used for bulk indexing.
     * @return a CompletableFuture that resolves to true if bulk indexing succeeds, or false otherwise.
     */
    private CompletableFuture<Boolean> bulkIndexChunksParallel(List<DocumentChunk> chunks, NodeClient client) {
        logger.info("Starting parallel bulk indexing process at " + new Date() + " for " + chunks.size() + " chunks");

        if (chunks == null || chunks.isEmpty()) {
            CompletableFuture<Boolean> emptyFuture = new CompletableFuture<>();
            emptyFuture.complete(true);
            return emptyFuture;
        }

        ExecutorService executor = Executors.newFixedThreadPool(MAX_CONCURRENT_BATCHES,
                r -> {
                    Thread t = new Thread(r);
                    t.setName("bulk-indexer-" + t.getId());
                    t.setPriority(Thread.MAX_PRIORITY);
                    return t;
                }
        );

        List<List<DocumentChunk>> batches = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i += BATCH_SIZE) {
            batches.add(new ArrayList<>(chunks.subList(i, Math.min(i + BATCH_SIZE, chunks.size()))));
        }

        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AtomicInteger completedBatches = new AtomicInteger(0);
        AtomicBoolean hasFailures = new AtomicBoolean(false);
        Semaphore semaphore = new Semaphore(MAX_CONCURRENT_BATCHES);
        int totalBatches = batches.size();

        logger.info("Divided into " + totalBatches + " batches, each with up to " +
                BATCH_SIZE + " chunks, processing up to " + MAX_CONCURRENT_BATCHES + " batches concurrently");

        for (int i = 0; i < batches.size(); i++) {
            final int batchIndex = i;
            final List<DocumentChunk> batch = batches.get(i);

            CompletableFuture.runAsync(() -> {
                try {
                    semaphore.acquire();
                    try {
                        processBatch(client, batch, batchIndex, totalBatches);
                    } catch (Exception e) {
                        logger.severe("Error processing batch " + (batchIndex + 1) + ": " + e.getMessage());
                        hasFailures.set(true);
                    } finally {
                        semaphore.release();
                        if (completedBatches.incrementAndGet() == totalBatches) {
                            executor.shutdown();
                            future.complete(!hasFailures.get());
                            logger.info("All " + totalBatches + " batches completed at " + new Date());
                        }
                    }
                } catch (InterruptedException e) {
                    logger.severe("Batch processing interrupted: " + e.getMessage());
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
     *
     * @param client       the NodeClient used for bulk indexing.
     * @param batch        the list of document chunks in the current batch.
     * @param batchIndex   the index of the current batch.
     * @param totalBatches the total number of batches.
     * @throws IOException if an I/O error occurs during bulk indexing.
     */
    private void processBatch(NodeClient client, List<DocumentChunk> batch, int batchIndex, int totalBatches) throws IOException {
        int batchNumber = batchIndex + 1;
        long startTime = System.currentTimeMillis();
        logger.info("Processing batch " + batchNumber + "/" + totalBatches + " with " + batch.size() + " chunks");

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout(org.elasticsearch.core.TimeValue.timeValueMinutes(5));

        for (DocumentChunk chunk : batch) {
            Map<String, Object> source = new HashMap<>();
            source.put("content", chunk.getContent());
            source.put("fileIdentifier", chunk.getFileIdentifier());
            source.put("fileName", chunk.getFileName());
            source.put("originalFilePath", chunk.getOriginalFilePath());
            source.put("sequenceNumber", chunk.getSequenceNumber());
            source.put("totalChunks", chunk.getTotalChunks());
            source.put("processedAt", chunk.getProcessedAt());
            source.put("fileSizeInBytes", chunk.getFileSizeInBytes());

            bulkRequest.add(createIndexRequest("target_index", chunk.getId(), source));
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(true);

        client.bulk(bulkRequest, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {
                try {
                    long timeTaken = System.currentTimeMillis() - startTime;
                    if (bulkResponse.hasFailures()) {
                        logger.warning("Batch " + batchNumber + "/" + totalBatches +
                                " completed with failures in " + (timeTaken / 1000.0) + "s: " +
                                bulkResponse.buildFailureMessage());
                        success.set(false);
                    } else {
                        logger.info("Batch " + batchNumber + "/" + totalBatches +
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
                    logger.severe("Batch " + batchNumber + "/" + totalBatches +
                            " failed after " + (timeTaken / 1000.0) + "s: " + e.getMessage());
                    success.set(false);
                } finally {
                    latch.countDown();
                }
            }
        });

        try {
            if (!latch.await(10, TimeUnit.MINUTES)) {
                logger.severe("Batch " + batchNumber + "/" + totalBatches + " timed out after 10 minutes");
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

    /**
     * Helper method to create an IndexRequest with the specified index name, document ID, and source map.
     *
     * @param indexName the name of the index.
     * @param id        the document ID.
     * @param source    the source map containing document fields.
     * @return an IndexRequest ready for indexing.
     */
    private IndexRequest createIndexRequest(String indexName, String id, Map<String, Object> source) {
        return new IndexRequest(indexName).id(id).source(source, XContentType.JSON);
    }

    /**
     * ProcessingResponse is a model representing the outcome of processing a TXT file.
     */
    public static class ProcessingResponse {
        private String id;
        private String filePath;
        private boolean success;
        private String errorMessage;
        private int chunkCount;
        private double processingTimeInSeconds;
        private long fileSizeInBytes;
        private int pageCount;
        private Map<String, Double> benchmarks;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getFilePath() { return filePath; }
        public void setFilePath(String filePath) { this.filePath = filePath; }
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        public int getChunkCount() { return chunkCount; }
        public void setChunkCount(int chunkCount) { this.chunkCount = chunkCount; }
        public double getProcessingTimeInSeconds() { return processingTimeInSeconds; }
        public void setProcessingTimeInSeconds(double processingTimeInSeconds) { this.processingTimeInSeconds = processingTimeInSeconds; }
        public long getFileSizeInBytes() { return fileSizeInBytes; }
        public void setFileSizeInBytes(long fileSizeInBytes) { this.fileSizeInBytes = fileSizeInBytes; }
        public int getPageCount() { return pageCount; }
        public void setPageCount(int pageCount) { this.pageCount = pageCount; }
        public Map<String, Double> getBenchmarks() { return benchmarks; }
        public void setBenchmarks(Map<String, Double> benchmarks) { this.benchmarks = benchmarks; }
    }

    /**
     * DocumentChunk represents a chunk of the processed file.
     */
    public static class DocumentChunk {
        // A unique identifier for this chunk, typically generated as a UUID.
        private String id;

        // An identifier derived from the original file's properties (e.g., name, size, modification time)
        // to uniquely relate the chunk back to the source file.
        private String fileIdentifier;

        // The complete file path of the original file from which this chunk was extracted.
        private String originalFilePath;

        // The name of the file (without path) from which this chunk originates.
        private String fileName;

        // The sequential number of this chunk in the overall file processing.
        private int sequenceNumber;

        // The total number of chunks into which the file was divided.
        private int totalChunks;

        // The starting page number associated with this chunk (useful if the file is viewed in pages).
        private int startPage;

        // The ending page number associated with this chunk.
        private int endPage;

        // The total number of pages in the original file, if applicable.
        private int totalPages;

        // The actual text content contained in this chunk.
        private String content;

        // The date and time when this chunk was processed.
        private Date processedAt;

        // The size of the original file in bytes.
        private long fileSizeInBytes;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getFileIdentifier() { return fileIdentifier; }
        public void setFileIdentifier(String fileIdentifier) { this.fileIdentifier = fileIdentifier; }
        public String getOriginalFilePath() { return originalFilePath; }
        public void setOriginalFilePath(String originalFilePath) { this.originalFilePath = originalFilePath; }
        public String getFileName() { return fileName; }
        public void setFileName(String fileName) { this.fileName = fileName; }
        public int getSequenceNumber() { return sequenceNumber; }
        public void setSequenceNumber(int sequenceNumber) { this.sequenceNumber = sequenceNumber; }
        public int getTotalChunks() { return totalChunks; }
        public void setTotalChunks(int totalChunks) { this.totalChunks = totalChunks; }
        public int getStartPage() { return startPage; }
        public void setStartPage(int startPage) { this.startPage = startPage; }
        public int getEndPage() { return endPage; }
        public void setEndPage(int endPage) { this.endPage = endPage; }
        public int getTotalPages() { return totalPages; }
        public void setTotalPages(int totalPages) { this.totalPages = totalPages; }
        public String getContent() { return content; }
        public void setContent(String content) { this.content = content; }
        public Date getProcessedAt() { return processedAt; }
        public void setProcessedAt(Date processedAt) { this.processedAt = processedAt; }
        public long getFileSizeInBytes() { return fileSizeInBytes; }
        public void setFileSizeInBytes(long fileSizeInBytes) { this.fileSizeInBytes = fileSizeInBytes; }
    }
}
