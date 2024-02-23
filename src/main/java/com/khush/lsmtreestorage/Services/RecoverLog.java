package com.khush.lsmtreestorage.Services;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class RecoverLog {
    private AVL avlTree;

    public RecoverLog(AVL avlTree) {
        this.avlTree = avlTree;
    }

    public void recover(String logFileName) throws IOException {
        String path = "./" + logFileName;
        Path logFilePath = Path.of(path);
        try(BufferedReader reader = Files.newBufferedReader(logFilePath)) {
            String line;
            while((line = reader.readLine()) != null) {
                String[] logEntry = line.split(", ");

                String[] log = logEntry[1].split(": ");
                String operation = log[0];
                String key = log[1];
                String value = log[2];

                if(operation.equals("INSERT")) {
                    avlTree.insert(key, value);
                } else if(operation.equals("UPDATE")) {
                    avlTree.update(key, value);
                } else {
                    avlTree.delete(key);
                }
            }
        }
    }
    
}
