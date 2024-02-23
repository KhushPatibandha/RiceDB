package com.khush.lsmtreestorage.Services;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class WriteAheadLog {
    private final Path LOG_FILE_PATH;
    private final FileChannel LOG_FILE_CHANNEL;
    private long byteOffset;

    public WriteAheadLog(String logFileName) throws IOException {
        this.LOG_FILE_PATH = Path.of(logFileName);
        this.LOG_FILE_CHANNEL = FileChannel.open(LOG_FILE_PATH, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.SYNC);
        this.byteOffset = 0;
    }

    public void writeInsertLog(String key, String value) {
        try {
            writeLog("INSERT", key, value);
        } catch (IOException e) {
            System.out.println("Error writing insert log");
            e.printStackTrace();
        }
    }

    public void writeUpdateLog(String key, String value) {
        try {
            writeLog("UPDATE", key, value);
        } catch (IOException e) {
            System.out.println("Error writing update log");
            e.printStackTrace();
        }
    }

    public void writeDeleteLog(String key) {
        try {
            writeLog("DELETE", key, "TOMBSTONE");
        } catch (IOException e) {
            System.out.println("Error writing delete log");
            e.printStackTrace();
        }
    }

    public void clearLog() {
        try(BufferedWriter writer = Files.newBufferedWriter(LOG_FILE_PATH)) {
            byteOffset = 0;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        LOG_FILE_CHANNEL.close();
    }

    private void writeLog(String operation, String key, String value) throws IOException {
        String logEntry = byteOffset + ", " + operation + ": " + key + ": " + value + "\n";
        byte[] bytes = logEntry.getBytes();
        LOG_FILE_CHANNEL.write(ByteBuffer.wrap(logEntry.getBytes()));
        byteOffset += bytes.length;
        fsync();
    }

    private void fsync() throws IOException {
        LOG_FILE_CHANNEL.force(true);
    }

}
