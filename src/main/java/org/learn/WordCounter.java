package org.learn;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WordCounter {
    static final String VERSION = "0.0.1";

    static String sourceDirectory;

    static final LinkedList<String> fileList = new LinkedList<>();
    static final LinkedList<Line> lines = new LinkedList<>();

    static final HashMap<String, Integer> wordCount = new HashMap<>();

    static int THREAD_COUNT = 5;

    static String MONGODB_URI = "mongodb://localhost:27017";
    static MongoClient mongoClient;
    static MongoDatabase database;
    static MongoCollection<Document> collection;

    public static void main(String[] args) {
        long startTime = System.nanoTime();
        log.info("Java multi-threaded word counter version :: {} ", VERSION);

        //STEP 0: Get source directory
        if (args.length >= 1) {
            sourceDirectory = args[0];
        } else {
            sourceDirectory = "/Users/rrajesh1979/Documents/Learn/gitrepo/word-count/java-wc-thread/src/main/resources/";
        }

        getMongoConnection();

        //STEP 1: Get list of files from source directory
        try {
            getFileList(sourceDirectory);
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("Number of files to count words :: {} ", fileList.size());

        ExecutorService fsExecutorService = Executors.newFixedThreadPool(THREAD_COUNT);
            //STEP 2: Read each file
            //STEP 3: Add lines from file to queue
        for (int i = 0; i < THREAD_COUNT; i++) {
            fsExecutorService.execute(new SplitFileRunnable());
        }

        awaitTerminationAfterShutdown(fsExecutorService);

        //STEP 4: Get each line and count # of words
        ExecutorService wcExecutorService = Executors.newFixedThreadPool(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            wcExecutorService.execute(new WordCountRunnable());
        }

        awaitTerminationAfterShutdown(wcExecutorService);

        log.info(wordCount.toString());
        long elapsedTime = System.nanoTime() - startTime;
        log.info("Total time taken :: {} milli-seconds", elapsedTime/1000000);
    }

    public static void getFileList(String dir) throws IOException {
        Files.walkFileTree(Paths.get(dir), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                if (!Files.isDirectory(file)) {
                    fileList.add(file.getFileName().toString());
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static List<String> readFile(String file) throws IOException {
        Path path = Paths.get(sourceDirectory + file);
        List<String> lines = null;
        if (file != null) {
            lines = Files.readAllLines(path);
        }
        return lines;
    }

    static class SplitFileRunnable implements Runnable {

        @Override
        public void run() {
            log.info("Starting new SplitFileRunnable Thread!");

            while (true) {
                String fileName = getFile();
                if (fileName != null) {
                    addFileLinesToList(fileName);
                } else {
                    break;
                }
            }
            Thread.currentThread().interrupt();

        }

        public String getFile() {
            synchronized (fileList) {
                if (!fileList.isEmpty()) {
                    return fileList.removeFirst();
                }
            }
            return null;
        }
    }

    public static void addFileLinesToList(String file) {
        try {
            List<String> linesIn = readFile(file);
            for (String line: linesIn) {
                synchronized (lines) {
                    lines.addFirst(new Line(file, line));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class WordCountRunnable implements Runnable {

        @Override
        public void run() {
            log.info("Started new WordCountRunnable thread!!");
            while (true) {
                Line line = getLine();
                if (line != null) {
                    updateWordCount(line);
                } else {
                    break;
                }
            }
            Thread.currentThread().interrupt();
        }

        public Line getLine() {
            synchronized (lines) {
                if (!lines.isEmpty()) {
                    return lines.removeLast();
                }
            }
            return null;
        }

        public String updateWordCount(Line line) {
            int wordsInLine = new StringTokenizer(line.line).countTokens();
            synchronized (wordCount) {
                wordCount.merge(line.filePath, wordsInLine, Integer::sum);
            }
            insertLine(line);
            return "SUCCESS";
        }
    }

    public static void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException ex) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void getMongoConnection() {
        mongoClient = MongoClients.create(MONGODB_URI);
        database = mongoClient.getDatabase("learn");
        collection = database.getCollection("wordcount");
    }

    public static boolean insertLine(Line line) {
        Document doc = new Document("file", line.filePath)
                .append("line", line.line);
        try {
            collection.insertOne(doc);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }
}
