package com.khush.lsmtreestorage.Services;

public class Node {
    String key;
    String value;
    int height;
    Node left, right;

    Node(String key, String value) {
        this.key = key;
        this.value = value;
        this.height = 1;
    }
    
}
