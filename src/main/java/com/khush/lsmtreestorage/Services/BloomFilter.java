package com.khush.lsmtreestorage.Services;

import java.util.BitSet;

public class BloomFilter {
    private BitSet bitSet;
    private int bitSetSize;
    private int numberOfHashFunctions;

    public BloomFilter() {
        this.bitSetSize = 224668;
        this.bitSet = new BitSet(this.bitSetSize);
        this.numberOfHashFunctions = 10;
    }

    public void add(String key) {
        for(int i = 0; i < this.numberOfHashFunctions; i++) {
            int hashCode = getHash(key, i);
            bitSet.set(Math.abs(hashCode % bitSetSize));
        }
    }

    public boolean mightContains(String key) {
        for(int i = 0; i < this.numberOfHashFunctions; i++) {
            int hashCode = getHash(key, i);
            if(!bitSet.get(Math.abs(hashCode % bitSetSize))) {
                return false;
            }
        }
        return true;
    }

    private int getHash(String key, int i) {
        return key.hashCode() + i * key.length();
    }

}
