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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.core.TimeValue;

public class TxtProcessingRestHandler extends BaseRestHandler {

    // Constants (ניתן להחליף בהגדרות חיצוניות)
    private static final int DEFAULT_CHUNK_SIZE_BYTES = 9 * 1024 * 1024; // 9 MB
    private static final int MAX_PARALLELISM = 4;
    private static final int MAX_CONCURRENT_BATCHES = 5; // Increased from 3 to 5
    private static final int BATCH_SIZE = 30; // Increased from 20 to 30 chunks per batch

    // סף לקבצים גדולים – מעל 50 MB נעשה שימוש בעיבוד מקבילי
    private static final long LARGE_FILE_THRESHOLD = 50L * 1024 * 1024; // 50 MB

    // ספריה מותרת לעיבוד קבצים
    private static final String ALLOWED_DIRECTORY = "C:/Users/BarGabay/big files";
    private static final Logger logger = Logger.getLogger(TxtProcessingRestHandler.class.getName());

    @Override
    public String getName() {
        return "txt_processing_rest_handler";
    }

    @Override
    public List<Route> routes() {
        // שני נתיבים – הנתיב הרגיל ובנוסף נתיב שמכריח שימוש תמידי בעיבוד סדרתי
        return Arrays.asList(
                new Route(RestRequest.Method.POST, "/_process_txt"),
                new Route(RestRequest.Method.POST, "/_process_txt_optimized")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try {
            // פענוח תוכן הבקשה – מצפים לשדה "path"
            Map<String, Object> sourceAsMap = request.contentParser().map();
            String filePath = (String) sourceAsMap.get("path");
            if (filePath == null) {
                throw new IllegalArgumentException("Missing 'path' parameter");
            }

            // אימות שהקובץ נמצא בתוך הספריה המותרת
            File requestedFile = new File(filePath);
            File allowedDir = new File(ALLOWED_DIRECTORY);
            if (!requestedFile.getCanonicalPath().startsWith(allowedDir.getCanonicalPath())) {
                throw new SecurityException("Access denied: File is outside allowed directory");
            }

            // עיבוד הקובץ
            return channel -> {
                try {
                    String uri = request.getHttpRequest().uri();
                    ProcessingResponse response;
                    if (uri.contains("_process_txt_optimized")) {
                        logger.info("Using always-optimized sequential processing for file: " + filePath);
                        response = processFileOptimized(filePath, client);
                    } else {
                        response = processFile(filePath, client);
                    }

                    // בניית תגובת JSON
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
     * If an index "target_index" exists, it is deleted, then a new one is created with optimized settings and mapping.
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

        // Apply mapping to the index.
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

            int chunkSizeInBytes = DEFAULT_CHUNK_SIZE_BYTES;
            List<DocumentChunk> chunks;
            // בחירה בין שיטת עיבוד על סמך גודל הקובץ
            if (fileSize > LARGE_FILE_THRESHOLD) {
                logger.info("File size (" + fileSize + " bytes) exceeds threshold (" + LARGE_FILE_THRESHOLD + " bytes). Using parallel processing.");
                chunks = chunkTextFileParallel(filePath, fileId, chunkSizeInBytes);
            } else {
                logger.info("File size (" + fileSize + " bytes) is within threshold. Using optimized sequential processing.");
                chunks = chunkTextFileOptimized(filePath, fileId, chunkSizeInBytes);
            }
            response.setChunkCount(chunks.size());

            long totalProcessedChars = chunks.stream().mapToLong(c -> c.getContent().length()).sum();
            logger.info("Total processed characters: " + totalProcessedChars + ", Original file size (bytes): " + fileSize);

            try {
                prepareOptimizedIndex(client);
            } catch (Exception e) {
                logger.warning("Could not optimize index settings: " + e.getMessage());
            }

            CompletableFuture<Boolean> bulkFuture = bulkIndexChunksParallel(chunks, client);
            boolean bulkResult = bulkFuture.get();
            if (!bulkResult) {
                response.setErrorMessage("Bulk indexing failed");
                return response;
            }

            // -------------------------------
            // שלב: ודא שכל המסמכים כבר זמינים לחיפוש
            long endOfIndexingMillis = System.currentTimeMillis();

            // 1. Refresh לאינדקס
            try {
                client.admin().indices().prepareRefresh("target_index").execute().actionGet();
                logger.info("Index refresh completed for target_index");
            } catch (Exception e) {
                logger.warning("Failed to refresh index: " + e.getMessage());
            }

            // 2. בדיקת ספירת המסמכים (במידה וכל צ'אנק מיוצג במסמך נפרד)
            try {
                long docCount = client.prepareSearch("target_index")
                        .setSize(0) // אין צורך להחזיר תוצאות, רק ספירה
                        .get()
                        .getHits()
                        .getTotalHits()
                        .value;
                if (docCount == chunks.size()) {
                    logger.info("All " + chunks.size() + " chunks are available for search in target_index");
                } else {
                    logger.warning("Expected " + chunks.size() + " documents, but found " + docCount + " in target_index");
                }
            } catch (Exception e) {
                logger.warning("Unable to verify final document count: " + e.getMessage());
            }

            // 3. (אופציונלי) בדיקת בריאות הקלאסטר לסטטוס Green
            try {
                ClusterHealthResponse healthResponse = client.admin()
                        .cluster()
                        .prepareHealth("target_index")
                        .setWaitForStatus(ClusterHealthStatus.GREEN)
                        .setTimeout(TimeValue.timeValueSeconds(30))
                        .execute()
                        .actionGet();

                if (healthResponse.getStatus() == ClusterHealthStatus.GREEN) {
                    logger.info("Cluster health is green for target_index. Index is ready.");
                } else {
                    logger.warning("Cluster health check returned non-green status: " + healthResponse.getStatus());
                }
            } catch (Exception ex) {
                logger.warning("Error checking cluster health: " + ex.getMessage());
            }

            // 4. חישוב זמן מהסיום של ה-bulk indexing ועד שהנתונים זמינים
            long finalTimeMillis = System.currentTimeMillis() - endOfIndexingMillis;
            logger.info("Time from end of bulk indexing to full availability: " + (finalTimeMillis / 1000.0) + "s");
            // -------------------------------

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

    private ProcessingResponse processFileOptimized(String filePath, NodeClient client) {
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

            int chunkSizeInBytes = DEFAULT_CHUNK_SIZE_BYTES;
            // תמיד להשתמש בשיטת העיבוד הסדרתית
            logger.info("Using optimized sequential processing for file: " + filePath);
            List<DocumentChunk> chunks = chunkTextFileOptimized(filePath, fileId, chunkSizeInBytes);
            response.setChunkCount(chunks.size());

            long totalProcessedChars = chunks.stream().mapToLong(c -> c.getContent().length()).sum();
            logger.info("Total processed characters: " + totalProcessedChars + ", Original file size (bytes): " + fileSize);

            try {
                prepareOptimizedIndex(client);
            } catch (Exception e) {
                logger.warning("Could not optimize index settings: " + e.getMessage());
            }

            CompletableFuture<Boolean> bulkFuture = bulkIndexChunksParallel(chunks, client);
            boolean bulkResult = bulkFuture.get();
            if (!bulkResult) {
                response.setErrorMessage("Bulk indexing failed");
                return response;
            }

            // -------------------------------
            // ודא שכל המסמכים זמינים לחיפוש
            long endOfIndexingMillis = System.currentTimeMillis();
            try {
                client.admin().indices().prepareRefresh("target_index").execute().actionGet();
                logger.info("Index refresh completed for target_index");
            } catch (Exception e) {
                logger.warning("Failed to refresh index: " + e.getMessage());
            }

            try {
                long docCount = client.prepareSearch("target_index")
                        .setSize(0)
                        .get()
                        .getHits()
                        .getTotalHits()
                        .value;
                if (docCount == chunks.size()) {
                    logger.info("All " + chunks.size() + " chunks are available for search in target_index");
                } else {
                    logger.warning("Expected " + chunks.size() + " documents, but found " + docCount + " in target_index");
                }
            } catch (Exception e) {
                logger.warning("Unable to verify final document count: " + e.getMessage());
            }

            long finalTimeMillis = System.currentTimeMillis() - endOfIndexingMillis;
            logger.info("Time from end of bulk indexing to full availability: " + (finalTimeMillis / 1000.0) + "s");
            // -------------------------------

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
            } catch (Exception ignored) { }
            try {
                Method getCleanerMethod = buffer.getClass().getDeclaredMethod("cleaner");
                getCleanerMethod.setAccessible(true);
                Object cleaner = getCleanerMethod.invoke(buffer);
                Method cleanMethod = cleaner.getClass().getDeclaredMethod("clean");
                cleanMethod.setAccessible(true);
                cleanMethod.invoke(cleaner);
                return;
            } catch (Exception ignored) { }
            try {
                Field unsafeField = Class.forName("sun.misc.Unsafe").getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                Object unsafe = unsafeField.get(null);
                Method invokeCleaner = unsafe.getClass().getMethod("invokeCleaner", ByteBuffer.class);
                invokeCleaner.invoke(unsafe, buffer);
                return;
            } catch (Exception ignored) { }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to clean direct buffer", e);
        }
        buffer.clear();
        System.gc();
    }

    /**
     * Enhanced version of bulkIndexChunksParallel for improved performance.
     * Divides document chunks into batches and indexes them in parallel.
     */
    private CompletableFuture<Boolean> bulkIndexChunksParallel(List<DocumentChunk> chunks, NodeClient client) {
        logger.info("Starting parallel bulk indexing process at " + new Date() + " for " + chunks.size() + " chunks");
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
     */
    private IndexRequest createIndexRequest(String indexName, String id, Map<String, Object> source) {
        return new IndexRequest(indexName).id(id).source(source, XContentType.JSON);
    }

    // Inner classes for the response and chunk models.

    /**
     * ProcessingResponse represents the outcome of processing a TXT file.
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
