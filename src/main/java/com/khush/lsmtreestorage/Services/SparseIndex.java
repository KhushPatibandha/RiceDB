package com.khush.lsmtreestorage.Services;

import java.io.RandomAccessFile;
import java.util.AbstractMap;
import java.util.Map;
import java.util.TreeMap;

public class SparseIndex {
    private TreeMap<String, Long> index;

    public SparseIndex() {
        this.index = new TreeMap<>();
    }

    public void add(String key, Long value) {
        this.index.put(key, value);
    }

    public Long get(String key) {
        return this.index.get(key);
    }

    public String findSegment(String key) {
        Map.Entry<String, Long> entry = index.floorEntry(key);
        return entry == null ? null : entry.getKey();
    }

    public Map.Entry<String, Long> highEntry(String key, RandomAccessFile raf) {
        String nextKey = index.higherKey(key);
        if(nextKey == null) {
            Long lastLineOffset = findLastLineOffset(raf);
            return new AbstractMap.SimpleEntry<>(key, lastLineOffset);
        }
        return new AbstractMap.SimpleEntry<>(nextKey, index.get(nextKey));
    }

    private Long findLastLineOffset(RandomAccessFile raf) {
        Long lastLineOffset = 0l;
        try {
            lastLineOffset = raf.length() - 1;
            raf.seek(lastLineOffset);
            while(raf.read() != '\n' && lastLineOffset > 0) {
                raf.seek(--lastLineOffset);
            }
        } catch (Exception e) {
            System.out.println("Error finding last line offset");
            e.printStackTrace();
        }
        return lastLineOffset;
    }
    
}
