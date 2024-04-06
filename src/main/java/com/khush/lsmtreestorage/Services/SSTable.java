package com.khush.lsmtreestorage.Services;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SSTable {
    private static Map<String, SSTable> fileNameAndSSTableMap = new HashMap<>();
    private static final String SSTABLE_DIRECTORY = "./sstable";
    private static final String LOG_FILE_NAME = "log.txt";
    private static final int SEGMENT_SIZE = 10 * 1024;
    private static final int MAX_TREE_SIZE = 16 * 1024;
    private static AVL avlTree;
    private static WriteAheadLog writeAheadLog;
    private static Compaction compaction;
    private static int currentTreeSize;
    private SparseIndex sparseIndex;
    private BloomFilter bloomFilter;
    private long byteOffset;
    private String fileName;

    static {
        avlTree = new AVL();
        try {
            writeAheadLog = new WriteAheadLog(LOG_FILE_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
        compaction = new Compaction();
    }

    public SSTable() {
        this.sparseIndex = new SparseIndex();
        this.bloomFilter = new BloomFilter();
        byteOffset = 0;
        currentTreeSize = 0;
        this.fileName = "sstable" + getSSTableCountPlusOne() + ".txt";
    }

    public SSTable(String fileName) {
        this.sparseIndex = new SparseIndex();
        this.bloomFilter = new BloomFilter();
        byteOffset = 0;
        currentTreeSize = 0;
        this.fileName = fileName;
    }

    public String getFileName() {
        return this.fileName;
    }

    public void setFileName(String name) {
        this.fileName = name;
    }

    public SparseIndex getSparseIndex() {
        return this.sparseIndex;
    }

    public void setSparseIndex(SparseIndex sparseIndex) {
        this.sparseIndex = sparseIndex;
    }

    public BloomFilter getBloomFilter() {
        return this.bloomFilter;
    }

    public void setBloomFilter(BloomFilter bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    public void recover() throws IOException {
        File file = new File(SSTABLE_DIRECTORY + "/" + this.fileName);
        byteOffset = file.length();
        sparseIndex = rebuildSparseIndex();
        bloomFilter = rebuildBloomFilter();
    }

    public SparseIndex rebuildSparseIndex() throws IOException {
        SparseIndex sIndex = new SparseIndex();
        byteOffset = 0;
        Path dirPath = Paths.get(SSTABLE_DIRECTORY);
        Path filePath = dirPath.resolve(fileName);

        try(BufferedReader reader = Files.newBufferedReader(filePath)) {
            int chunkSize = 0;
            String firstKeyInChunk = null;
            String line;

            while((line = reader.readLine()) != null) {
                String key = line.split(":")[0].trim();
                int lineSize = line.getBytes().length + 1;

                if(chunkSize + lineSize <= SEGMENT_SIZE) {
                    if(firstKeyInChunk == null) {
                        firstKeyInChunk = key;
                    }
                    chunkSize += lineSize;
                } else {
                    sIndex.add(firstKeyInChunk, byteOffset);
                    byteOffset += chunkSize;
                    chunkSize = lineSize;
                    firstKeyInChunk = key;
                }
            }

            if(firstKeyInChunk != null) {
                sIndex.add(firstKeyInChunk, byteOffset);
                byteOffset += chunkSize;
            }
            reader.close();
        }
        return sIndex;
    }

    public BloomFilter rebuildBloomFilter() throws IOException {
        BloomFilter bloomFilter = new BloomFilter();
        BufferedReader reader = new BufferedReader(new FileReader(SSTABLE_DIRECTORY + "/" + fileName));
        String line;

        while((line = reader.readLine()) != null) {
            String key = line.split(": ")[0];
            bloomFilter.add(key);
        }
        reader.close();
        return bloomFilter;
    }

    public static AVL getAvlTree() {
        return avlTree;
    }

    public static void insert(String key, String value) throws IOException {
        if(avlTree.findKey(key) == false) {
            writeAheadLog.writeInsertLog(key, value);
            avlTree.insert(key, value);
            currentTreeSize += (key.getBytes().length + value.getBytes().length);
            if(currentTreeSize >= MAX_TREE_SIZE) {
                flushToSSTable();
                avlTree.empty();
                currentTreeSize = 0;
            }
        } else {
            update(key, value);
        }
    }

    public static void update(String key, String value) throws IOException {
        if(avlTree.findKey(key) == true) {
            String oldValue = avlTree.findValue(key);
            writeAheadLog.writeUpdateLog(key, value);
            avlTree.update(key, value);
            currentTreeSize -= oldValue.getBytes().length;
            currentTreeSize += value.getBytes().length;
        } else {
            insert(key, value);
        }
    }

    public static void delete(String key) throws IOException {
        if(avlTree.findKey(key) == true) {
            String oldValue = avlTree.findValue(key);
            writeAheadLog.writeDeleteLog(key);
            avlTree.update(key, "TOMBSTONE");
            currentTreeSize -= oldValue.getBytes().length;
            currentTreeSize += "TOMBSTONE".getBytes().length;
        } else {
            insert(key, "TOMBSTONE");
        }
    }

    public static void flushToSSTable() throws IOException {
        SSTable sstable = new SSTable();
        sstable.byteOffset = 0;
        Path dirPath = Paths.get(SSTABLE_DIRECTORY);
        if(!Files.exists(dirPath)) {
            Files.createDirectory(dirPath);
        }
        Path filePath = dirPath.resolve(sstable.getFileName());
        try(BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            List<List<String>> keyValuePair = avlTree.getInOrderTraversal();
            int chunkSize = 0;
            String firstKeyInChunk = null;
            for(List<String> pair : keyValuePair) {
                String key = pair.get(0);
                String value = pair.get(1);
                String line = key + ": " + value + "\n";
                int lineSize = line.getBytes().length;
                sstable.bloomFilter.add(key);

                if(chunkSize + lineSize <= SEGMENT_SIZE) {
                    if(firstKeyInChunk == null) {
                        firstKeyInChunk = key;
                    }
                    chunkSize += lineSize;
                } else {
                    sstable.sparseIndex.add(firstKeyInChunk, sstable.byteOffset);
                    sstable.byteOffset += chunkSize;
                    chunkSize = lineSize;
                    firstKeyInChunk = key;
                }

                writer.write(line);
            }
            sstable.sparseIndex.add(firstKeyInChunk, sstable.byteOffset);
            writeAheadLog.clearLog();
        }
        fileNameAndSSTableMap.put(sstable.getFileName(), sstable);
        compaction.addSSTable(sstable);
        compaction.compactIfNeeded();
    }

    public static String readKey(String key) throws IOException {
        String value = avlTree.findValue(key);
        if(value != null) {
            return value;
        }

        File dir = new File(SSTABLE_DIRECTORY);
        File[] files = dir.listFiles((d, name) -> name.startsWith("sstable") && name.endsWith(".txt"));

        if(files == null) {
            return null;
        }

        Arrays.sort(files, (file1, file2) -> {
            int number1 = Integer.parseInt(file1.getName().substring(7, file1.getName().length() - 4));
            int number2 = Integer.parseInt(file2.getName().substring(7, file2.getName().length() - 4));
            return Integer.compare(number2, number1);
        });

        String result = null;
        for(File file : files) {
            String sstableFilename = file.getName();
            SSTable sstable = fileNameAndSSTableMap.get(sstableFilename);

            if(sstable.bloomFilter.mightContains(key) == false) {
                continue;
            }

            SparseIndex sIndex = sstable.getSparseIndex();
            String segmentStartKey = sIndex.findSegment(key);
            if(segmentStartKey != null) {
                Path filePath = Paths.get(SSTABLE_DIRECTORY, sstable.getFileName());
                RandomAccessFile raf = null;
                try {
                    raf = new RandomAccessFile(filePath.toFile(), "r");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                long startOffset = sIndex.get(segmentStartKey);
                long endOffset = sIndex.highEntry(segmentStartKey, raf).getValue();

                long byteOffset = binarySeachSSTable(sstableFilename, startOffset, endOffset, key);

                if(byteOffset != -1) {
                    raf.seek(byteOffset);
                    seekToBeginningOfLine(raf);
                    String line = raf.readLine();
                    result = line.split(": ")[1];
                    return result;
                }

                if(result != null) {
                    break;
                }
            }
        }
        return result;
    }

    public static List<String> readRange(String start, String end) throws IOException {
        File dir = new File(SSTABLE_DIRECTORY);
        File[] files = dir.listFiles((d, name) -> name.startsWith("sstable") && name.endsWith(".txt"));

        if(files == null) {
            return null;
        }

        Arrays.sort(files, (file1, file2) -> {
            int number1 = Integer.parseInt(file1.getName().substring(7, file1.getName().length() - 4));
            int number2 = Integer.parseInt(file2.getName().substring(7, file2.getName().length() - 4));
            return Integer.compare(number2, number1);
        });

        List<String> result = new ArrayList<>();
        for(File file : files) {
            String sstableFilename = file.getName();
            SSTable sstable = fileNameAndSSTableMap.get(sstableFilename);

            if(sstable.bloomFilter.mightContains(start) == false && sstable.bloomFilter.mightContains(end) == false) {
                continue;
            } else if(sstable.bloomFilter.mightContains(start) == true && sstable.bloomFilter.mightContains(start) == true) {
                if(start.compareTo(end) < 0) {
                    SparseIndex sIndex = sstable.getSparseIndex();
                    String segmentStartKey = sIndex.findSegment(start);
                    if(segmentStartKey != null) {
                        Path filePath = Paths.get(SSTABLE_DIRECTORY, sstable.getFileName());
                        RandomAccessFile raf = null;
                        try {
                            raf = new RandomAccessFile(filePath.toFile(), "r");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        long startOffset = sIndex.get(segmentStartKey);
                        long endOffset = sIndex.highEntry(segmentStartKey, raf).getValue();

                        long byteOffset = binarySeachSSTable(sstableFilename, startOffset, endOffset, start);

                        if(byteOffset != -1) {
                            raf.seek(byteOffset);
                            seekToBeginningOfLine(raf);
                            String line = raf.readLine();
                            String currValue = line.split(": ")[1];
                            result.add(currValue);
                        }
                        
                        String line;
                        while((line = raf.readLine()) != null) {
                            String currKey = line.split(": ")[0];
                            if(currKey.compareTo(end) <= 0) {
                                String currValue = line.split(": ")[1];
                                result.add(currValue);
                            } else {
                                break;
                            }
                        }
                        raf.close();
                        return result;
                    }
                } else { // start.compareTo(end) > 0
                    SparseIndex sIndex = sstable.getSparseIndex();
                    String segmentStartKey = sIndex.findSegment(start);
                    if(segmentStartKey != null) {
                        Path filePath = Paths.get(SSTABLE_DIRECTORY, sstable.getFileName());
                        RandomAccessFile raf = null;
                        try {
                            raf = new RandomAccessFile(filePath.toFile(), "r");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        long startOffset = sIndex.get(segmentStartKey);
                        long endOffset = sIndex.highEntry(segmentStartKey, raf).getValue();

                        long byteOffset = binarySeachSSTable(sstableFilename, startOffset, endOffset, end);

                        if(byteOffset != -1) {
                            raf.seek(byteOffset);
                            seekToBeginningOfLine(raf);
                            String line = raf.readLine();
                            String currValue = line.split(": ")[1];
                            result.add(currValue);
                        }
                        
                        String line;
                        while((line = raf.readLine()) != null) {
                            String currKey = line.split(": ")[0];
                            if(currKey.compareTo(start) <= 0) {
                                String currValue = line.split(": ")[1];
                                result.add(currValue);
                            } else {
                                break;
                            }
                        }
                        raf.close();
                        return result;
                    }
                }
            } else if(sstable.bloomFilter.mightContains(start) == false && sstable.bloomFilter.mightContains(end) == true) {
                
            } else { // start = true and end = false

            }
        }

        return null;
    }

    public static void updateFileNameAndSSTableMap(String oldFileName1, String oldFileName2, String newFileName, SSTable newSSTable) {
        fileNameAndSSTableMap.remove(oldFileName1);
        fileNameAndSSTableMap.remove(oldFileName2);
        fileNameAndSSTableMap.put(newFileName, newSSTable);
    }

    public static void recoverFileNameAndSSTableMap() throws IOException {
        File directory = new File(SSTABLE_DIRECTORY);
        if(directory.exists() && directory.isDirectory()) {
            for(String fileName : directory.list()) {
                SSTable sstable = new SSTable(fileName);
                sstable.recover();
                fileNameAndSSTableMap.put(fileName, sstable);
            }
        }
    }

    public static int getNumberOfSSTableCount() {
        File directory = new File(SSTABLE_DIRECTORY);
        if(directory.exists() && directory.isDirectory()) {
            return directory.list().length;
        }
        return 0;
    }

    public static int getSSTableCount() {
        File directory = new File(SSTABLE_DIRECTORY);
        int highestNumber = 0;
        if (directory.exists() && directory.isDirectory()) {
            for (String fileName : directory.list()) {
                if (fileName.startsWith("sstable")) {
                    int number = Integer.parseInt(fileName.substring(7, fileName.length() - 4));
                    if (number > highestNumber) {
                        highestNumber = number;
                    }
                }
            }
        }
        return highestNumber;
    }

    public static void close() throws IOException {
        writeAheadLog.close();
    }

    private static long binarySeachSSTable(String sstableFileName, long startOffset, long endOffset, String key) throws IOException {
        Path filePath = Paths.get(SSTABLE_DIRECTORY, sstableFileName);

        long low = startOffset;
        long high = endOffset;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(filePath.toFile(), "r");
        } catch (Exception e) {
            e.printStackTrace();
        }

        while(low <= high) {
            long mid = low + (high - low) / 2;
            try {
                raf.seek(mid);
            } catch (Exception e) {
                e.printStackTrace();
            }
            mid = seekToBeginningOfLine(raf);

            String line = raf.readLine();
            String currKey = line.split(": ")[0];

            if(currKey.equals(key)) {
                raf.close();
                return mid;
            } else if(currKey.compareTo(key) < 0) {
                low = raf.getFilePointer();
            } else {
                high = mid - 1;
                if (high > 0) {
                    raf.seek(high);
                    high = seekToBeginningOfLine(raf);
                }
            }
        }

        raf.close();
        return -1;
    }

    private static long seekToBeginningOfLine(RandomAccessFile raf) throws IOException {
        long currentPosition = raf.getFilePointer();
        while (currentPosition > 0) {
            raf.seek(currentPosition - 1);
            if (raf.readByte() == '\n') {
                return currentPosition;
            }
            currentPosition--;
        }
        raf.seek(0);
        return 0;
    }

    private static int getSSTableCountPlusOne() {
        return getSSTableCount() + 1;
    }

}
