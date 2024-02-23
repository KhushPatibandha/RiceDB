package com.khush.lsmtreestorage.Services;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Compaction {
    private static final int COMPACTION_THRESHOLD = 5;
    private static final String SSTABLE_DIRECTORY = "./sstable";
    private static Map<Integer, List<SSTable>> sstableBuckets = new HashMap<>();

    public Compaction() {
        for(int i = 0; i < 4; i++) {
            sstableBuckets.put(i, new ArrayList<>());
        }
    }

    public void addSSTable(SSTable sstable) {
        String sstableName = sstable.getFileName();
        String filePath = SSTABLE_DIRECTORY + "/" + sstableName;
        File file = new File(filePath);
        long sizeInBytes = file.length();

        int bucket = getBucket(sizeInBytes);
        List<SSTable> sstables = sstableBuckets.get(bucket);
        sstables.add(sstable);
        sstableBuckets.put(bucket, sstables);
    }

    public void compactIfNeeded() throws IOException {
        for(int bucket = 0; bucket < sstableBuckets.size(); bucket++) {
            if(sstableBuckets.get(bucket).size() > COMPACTION_THRESHOLD) {
                compactFilesInBucket(bucket);
            }
        }
    }

    public void rebuildBuckets() {
        File dir = new File(SSTABLE_DIRECTORY);
        File[] files = dir.listFiles();
        if(files != null) {
            for(File file : files) {
                SSTable sstable = new SSTable(file.getName());
                addSSTable(sstable);
            }
        }
    }

    public void moveSSTableToDifferentBucket(SSTable sstable) {
        for(List<SSTable> sstables : sstableBuckets.values()) {
            sstables.remove(sstable);
        }
        addSSTable(sstable);
    }

    public void printAllBucketWithSSTablesName() {
        for(Map.Entry<Integer, List<SSTable>> entry : sstableBuckets.entrySet()) {
            System.out.println("Bucket: " + entry.getKey());
            for(SSTable sstable : entry.getValue()) {
                System.out.println(sstable.getFileName());
            }
        }
    }

    private void compactFilesInBucket(int bucket) throws IOException {
        Random random = new Random();
        int randomNum = random.nextInt(100);
        List<SSTable> sstables = sstableBuckets.get(bucket);

        if(sstables != null && sstables.size() > 1) {
            Collections.sort(sstables, Comparator.comparingInt(sstable -> Integer.parseInt(sstable.getFileName().substring(7, sstable.getFileName().length() - 4))));

            String firstFileName = sstables.get(0).getFileName();
            String secondFileName = sstables.get(1).getFileName();
            String tempFileName = "temp" + randomNum + ".txt";

            String file1Path = SSTABLE_DIRECTORY + "/" + firstFileName;
            String file2Path = SSTABLE_DIRECTORY + "/" + secondFileName;
            String tempFilePath = SSTABLE_DIRECTORY + "/" + tempFileName;

            SSTable tempSSTable = new SSTable(tempFileName);

            compact(firstFileName, secondFileName, tempFileName, tempSSTable);

            Files.deleteIfExists(Paths.get(file1Path));
            Files.deleteIfExists(Paths.get(file2Path));

            tempSSTable.setFileName(secondFileName);
            Files.move(Paths.get(tempFilePath), Paths.get(file2Path));
            SparseIndex sIndex = tempSSTable.rebuildSparseIndex();
            tempSSTable.setSparseIndex(sIndex);
            SSTable.updateFileNameAndSSTableMap(firstFileName, secondFileName, secondFileName, tempSSTable);

            sstables.removeIf(sstable -> sstable.getFileName().equals(firstFileName) || sstable.getFileName().equals(secondFileName));

            addSSTable(tempSSTable);
        }
    }

    private void compact(String file1, String file2, String outputFile, SSTable outputSSTable) throws IOException {
        String file1Path = SSTABLE_DIRECTORY + "/" + file1;
        String file2Path = SSTABLE_DIRECTORY + "/" + file2;
        String outputFilePath = SSTABLE_DIRECTORY + "/" + outputFile;

        BufferedReader reader1 = new BufferedReader(new FileReader(file1Path));
        BufferedReader reader2 = new BufferedReader(new FileReader(file2Path));
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath));

        String line1 = reader1.readLine();
        String line2 = reader2.readLine();

        while(line1 != null && line2 != null) {
            String[] pair1FromFile1 = line1.split(": ");
            String[] pair2FromFile2 = line2.split(": ");

            String key1 = pair1FromFile1[0];
            String value1 = pair1FromFile1[1];
            String key2 = pair2FromFile2[0];
            String value2 = pair2FromFile2[1];

            /*
             * (1) Both keys are different. => write smaller pair to the output file. move the pointer to +1 from that file
             * (2) Both keys are same. => write the pair the latest file to the output file. move the pointer to +1 from both files
             */
            
            int cmp = key1.compareTo(key2);
            if(cmp < 0) {
                if(!value1.equals("TOMBSTONE")) {
                    writer.write(line1);
                    writer.newLine();
                    outputSSTable.getBloomFilter().add(key1);
                }
                line1 = reader1.readLine();
            } else if(cmp > 0) {
                if(!value2.equals("TOMBSTONE")) {
                    writer.write(line2);
                    writer.newLine();
                    outputSSTable.getBloomFilter().add(key2);
                }
                line2 = reader2.readLine();
            } else {
                if(!value2.equals("TOMBSTONE")) {
                    writer.write(line2);
                    writer.newLine();
                    outputSSTable.getBloomFilter().add(key2);
                }
                line1 = reader1.readLine();
                line2 = reader2.readLine();
            }
        }

        while(line1 != null) {
            String[] pair1FromFile1 = line1.split(": ");
            String value1 = pair1FromFile1[1];
            if(!value1.equals("TOMBSTONE")) {
                writer.write(line1);
                writer.newLine();
                outputSSTable.getBloomFilter().add(pair1FromFile1[0]);
            }
            line1 = reader1.readLine();
        }

        while (line2 != null) {
            String[] pair2FromFile2 = line2.split(": ");
            String value2 = pair2FromFile2[1];
            if(!value2.equals("TOMBSTONE")) {
                writer.write(line2);
                writer.newLine();
                outputSSTable.getBloomFilter().add(pair2FromFile2[0]);
            }
            line2 = reader2.readLine();
        }

        reader1.close();
        reader2.close();
        writer.close();
    }

    private int getBucket(long sizeInBytes) {
        long sizeInKb = sizeInBytes / 1024;

        if(sizeInKb < 30) {
            return 0;
        } else if(sizeInKb < 60) {
            return 1;
        } else if(sizeInKb < 120) {
            return 2;
        } else {
            return 3;
        }
    }
    
}
