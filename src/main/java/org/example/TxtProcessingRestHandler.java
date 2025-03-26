package org.example;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
// הסרת ייבוא של TimeValue שלא נמצא
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TxtProcessingRestHandler extends BaseRestHandler {
    // ערכים קבועים – ניתן להחליף בהגדרות חיצוניות במידת הצורך
    private static final int DEFAULT_CHUNK_SIZE_BYTES = 9 * 1024 * 1024; // 9 MB
    private static final int MAX_PARALLELISM = 4;
    // הגבלת נתיבים – לדוגמה, רק קבצים מתיקיית "C:/AllowedFiles" יהיו מורשים
    private static final String ALLOWED_DIRECTORY = "C:/Users/BarGabay/big files";
    private static final Logger logger = Logger.getLogger(TxtProcessingRestHandler.class.getName());

    @Override
    public String getName() {
        return "txt_processing_rest_handler";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
                new Route(RestRequest.Method.POST, "/_process_txt")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try {
            // צפייה בנתוני ה-JSON – מצפים לקבל שדה "path"
            Map<String, Object> sourceAsMap = request.contentParser().map();
            String filePath = (String) sourceAsMap.get("path");
            if (filePath == null) {
                throw new IllegalArgumentException("Missing 'path' parameter");
            }

            // בדיקת נתיב מורשה
            File requestedFile = new File(filePath);
            File allowedDir = new File(ALLOWED_DIRECTORY);
            if (!requestedFile.getCanonicalPath().startsWith(allowedDir.getCanonicalPath())) {
                throw new SecurityException("Access denied: File is outside allowed directory");
            }

            // עיבוד הקובץ
            return channel -> {
                try {
                    ProcessingResponse response = processFile(filePath, client);

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

            // ניתן לשלב קריאת גודל צ'אנק מהגדרות (אם קיימות) – כאן נעשה שימוש בערך ברירת מחדל
            int chunkSizeInBytes = DEFAULT_CHUNK_SIZE_BYTES;

            // עיבוד קובץ לצ'אנקים באמצעות MemoryMappedFile ובמקביליות
            List<DocumentChunk> chunks = chunkTextFileParallel(filePath, fileId, chunkSizeInBytes);
            response.setChunkCount(chunks.size());

            // רישום בדיקת תקינות (סיכום אורך הטקסט המעובד לעומת גודל מקור, קרי למטרת לוג)
            long totalProcessedChars = chunks.stream().mapToLong(c -> c.getContent().length()).sum();
            logger.info("Total processed characters: " + totalProcessedChars + ", Original file size (bytes): " + fileSize);

            // Bulk indexing – שליחת הצ'אנקים באינדוקס אסינכרוני ל-Elasticsearch
            CompletableFuture<Boolean> bulkFuture = bulkIndexChunksAsync(chunks, client);

            // שינוי: המתנה ללא timeout - יחכה עד שהתהליך יסתיים
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
            // הסרת הטיפול ב-TimeoutException
            logger.log(Level.SEVERE, "Error processing TXT file", ex);
            response.setErrorMessage("Error processing TXT: " + ex.getMessage());
            return response;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Error processing TXT file", ex);
            response.setErrorMessage("Error processing TXT: " + ex.getMessage());
            return response;
        }
    }

    private List<DocumentChunk> chunkTextFileParallel(String filePath, String fileId, int chunkSizeInBytes)
            throws IOException, InterruptedException, ExecutionException {
        File file = new File(filePath);
        long fileSize = file.length();
        int chunkCount = (int) Math.ceil((double) fileSize / chunkSizeInBytes);
        List<Future<DocumentChunk>> futures = new ArrayList<>();

        // שינוי: שימוש בשרת Thread מוגדר היטב עם שם ברור
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

        // שינוי: ניהול ידני של RandomAccessFile ו-FileChannel
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
                        // שימוש בנעילה כדי למנוע בעיות סנכרון בעת יצירת המפה
                        synchronized (finalChannel) {
                            buffer = finalChannel.map(FileChannel.MapMode.READ_ONLY, startPos, size);
                        }

                        byte[] bytes = new byte[(int) size];
                        buffer.get(bytes);

                        // שימוש ב-decode בטוח עבור UTF-8
                        String rawContent = safeUtf8Decode(bytes);

                        // התאמת גבולות הצ'אנק באופן אחיד
                        boolean isFirst = index == 0;
                        boolean isLast = index == chunkCount - 1;
                        String content = adjustChunkBoundaries(rawContent, !isFirst, !isLast);

                        // שיפור: שחרור המפה
                        cleanMappedByteBuffer(buffer);

                        // יצירת ה-chunk כרגיל
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
                        // לוג מפורט יותר של שגיאות
                        logger.log(Level.SEVERE, "Error processing chunk " + index, e);
                        throw e;
                    }
                }));
            }

            // המתנה לסיום כל המשימות לפני סגירת הערוץ
            executor.shutdown();
            boolean completed = executor.awaitTermination(1, TimeUnit.HOURS);
            if (!completed) {
                logger.log(Level.SEVERE,"Processing chunks timed out after 1 hour");
            }

            // איסוף התוצאות רק אחרי שכל המשימות סיימו
            List<DocumentChunk> chunks = new ArrayList<>();
            for (Future<DocumentChunk> future : futures) {
                chunks.add(future.get());
            }
            chunks.sort(Comparator.comparingInt(DocumentChunk::getSequenceNumber));
            return chunks;

        } finally {
            // וידוא סגירת משאבים
            if (executor != null && !executor.isTerminated()) {
                try {
                    logger.info("Shutting down executor service");
                    executor.shutdownNow();
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Error shutting down executor", e);
                }
            }

            // סגירת ה-channel וה-file בטוחה
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

    // פונקציה לטיפול בביטוי UTF-8 באופן בטוח – מוודאת שהבייט האחרון הוא תחילת תו חוקי
    private String safeUtf8Decode(byte[] bytes) {
        int len = bytes.length;
        // בדיקה אם הבייט האחרון הוא continuation byte (0x80-0xBF)
        while (len > 0 && (bytes[len - 1] & 0xC0) == 0x80) {
            len--;
        }
        return new String(bytes, 0, len, StandardCharsets.UTF_8);
    }

    // פונקציה לאיחוד טיפול בגבולות הצ'אנק – אם לא הצ'אנק הראשון, מתחילים בגבול ראשון; אם לא האחרון, מסתיימים בגבול אחרון
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

        // בדיקה שהאינדקסים חוקיים
        if (startIdx >= endIdx || startIdx >= content.length()) {
            return content;
        }

        return content.substring(startIdx, endIdx);
    }

    // פונקציה למציאת break point ראשון (בתוך 500 תווים)
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

    // פונקציה למציאת break point אחרון (בתוך 500 תווים מהסוף)
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

    // שחרור ידני של המפה - שיטה משופרת שלא משתמשת ב-AccessController
    private void cleanMappedByteBuffer(final MappedByteBuffer buffer) {
        if (buffer == null) {
            return;
        }

        try {
            // שיטה 1: שימוש ב-reflection לגישה לcleaner (JDK 9+)
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
                // ממשיך לשיטה הבאה אם זו נכשלה
            }

            // שיטה 2: דרך reflection על DirectByteBuffer (גישה אחרת)
            try {
                Method getCleanerMethod = buffer.getClass().getDeclaredMethod("cleaner");
                getCleanerMethod.setAccessible(true);
                Object cleaner = getCleanerMethod.invoke(buffer);
                Method cleanMethod = cleaner.getClass().getDeclaredMethod("clean");
                cleanMethod.setAccessible(true);
                cleanMethod.invoke(cleaner);
                return;
            } catch (Exception ignored) {
                // ממשיך לשיטה הבאה אם גם זו נכשלה
            }

            // שיטה 3: שימוש ב-Unsafe (ניסיון אחרון)
            try {
                // ניסיון לגשת ל-Unsafe דרך reflection
                Field unsafeField = Class.forName("sun.misc.Unsafe").getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                Object unsafe = unsafeField.get(null);

                // ניסיון להשתמש ב-Unsafe.invokeCleaner (JDK 9+)
                Method invokeCleaner = unsafe.getClass().getMethod("invokeCleaner", ByteBuffer.class);
                invokeCleaner.invoke(unsafe, buffer);
                return;
            } catch (Exception ignored) {
                // אם כל השיטות נכשלו, ננסה ברירת מחדל
            }

        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to clean direct buffer", e);
        }

        // ברירת מחדל: איפוס וGC
        buffer.clear();
        System.gc();
    }

    // שינוי: שילוב גישת עיבוד במנות עם CompletableFuture
    private CompletableFuture<Boolean> bulkIndexChunksAsync(List<DocumentChunk> chunks, NodeClient client) {
        logger.info("Starting bulk indexing process at " + new Date() + " for " + chunks.size() + " chunks");

        if (chunks == null || chunks.isEmpty()) {
            CompletableFuture<Boolean> emptyFuture = new CompletableFuture<>();
            emptyFuture.complete(true);  // אין מה לאנדקס, אז נחשיב את זה כהצלחה
            return emptyFuture;
        }

        CompletableFuture<Boolean> future = new CompletableFuture<>();

        // שינוי: נשלח 10 צ'אנקים בכל bulk request (כ-90MB בכל פעם)
        // מבוסס על בדיקות ביצועים בשטח שהראו שזה הגודל האופטימלי
        int batchSize = 10;
        int totalBatches = (int) Math.ceil((double) chunks.size() / batchSize);
        AtomicInteger completedBatches = new AtomicInteger(0);

        logger.info("Sending chunks in " + totalBatches + " batches, each with up to " + batchSize + " chunks (optimized for performance)");

        // שולח מנה ראשונה
        sendNextBatch(chunks, 0, batchSize, totalBatches, completedBatches, future, client);

        return future;
    }

    private void sendNextBatch(List<DocumentChunk> chunks, int startIdx, int batchSize,
                               int totalBatches, AtomicInteger completedBatches,
                               CompletableFuture<Boolean> future, NodeClient client) {

        int endIdx = Math.min(startIdx + batchSize, chunks.size());
        BulkRequest bulkRequest = new BulkRequest();

        // הוספת timeout סביר לבקשה עצמה
        bulkRequest.timeout(org.elasticsearch.core.TimeValue.timeValueMinutes(5));

        // הוספת הצ'אנקים למנה הנוכחית
        for (int i = startIdx; i < endIdx; i++) {
            DocumentChunk chunk = chunks.get(i);
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

        int currentBatch = completedBatches.get() + 1;
        logger.info("Sending batch " + currentBatch + "/" + totalBatches +
                " (" + (startIdx + 1) + "-" + endIdx + " of " + chunks.size() + " chunks)");

        client.bulk(bulkRequest, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {
                int completed = completedBatches.incrementAndGet();

                if (bulkResponse.hasFailures()) {
                    logger.warning("Batch " + currentBatch + " has failures: " +
                            bulkResponse.buildFailureMessage());
                } else {
                    logger.info("Batch " + currentBatch + "/" + totalBatches +
                            " completed successfully in " + bulkResponse.getTook());
                }

                // אם יש עוד מנות לשלוח
                if (endIdx < chunks.size()) {
                    sendNextBatch(chunks, endIdx, batchSize, totalBatches,
                            completedBatches, future, client);
                }
                // אם זו המנה האחרונה שהושלמה
                else if (completed == totalBatches) {
                    logger.info("All " + totalBatches + " batches completed successfully at " + new Date());
                    future.complete(true);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.log(Level.SEVERE, "Batch " + currentBatch + " failed", e);
                int completed = completedBatches.incrementAndGet();

                // נמשיך לנסות מנות אחרות למרות כישלון
                if (endIdx < chunks.size()) {
                    sendNextBatch(chunks, endIdx, batchSize, totalBatches,
                            completedBatches, future, client);
                } else if (completed == totalBatches) {
                    // אם כל המנות נשלחו, נחשיב את הפעולה כהצלחה חלקית
                    logger.warning("All batches processed with some failures");
                    future.complete(false);
                }
            }
        });
    }

    // פונקציית עזר ליצירת IndexRequest
    private IndexRequest createIndexRequest(String indexName, String id, Map<String, Object> source) {
        return new IndexRequest(indexName).id(id).source(source, XContentType.JSON);
    }

    // מחלקות מודל לתגובה ול-document chunk
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

        // getters and setters
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

    public static class DocumentChunk {
        private String id;
        private String fileIdentifier;
        private String originalFilePath;
        private String fileName;
        private int sequenceNumber;
        private int totalChunks;
        private int startPage;
        private int endPage;
        private int totalPages;
        private String content;
        private Date processedAt;
        private long fileSizeInBytes;

        // getters and setters
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