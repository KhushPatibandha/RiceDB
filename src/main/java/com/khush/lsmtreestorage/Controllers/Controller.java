package com.khush.lsmtreestorage.Controllers;

import java.io.IOException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.khush.lsmtreestorage.Model.KeyValuePair;
import com.khush.lsmtreestorage.Services.RecoverLog;
import com.khush.lsmtreestorage.Services.SSTable;

@RestController
@RequestMapping("/api")
public class Controller {

    private static final String LOG_FILE_NAME = "log.txt";
    private final RecoverLog recoverLog;

    public Controller() {
        this.recoverLog = new RecoverLog(SSTable.getAvlTree());
    }

    @PostMapping("/insert")
    public ResponseEntity<String> insert(@RequestBody KeyValuePair pair) throws IOException {
        SSTable.insert(pair.getKey(), pair.getValue());
        return new ResponseEntity<>("Value Inserted", HttpStatus.CREATED);
    }

    @PutMapping("/update")
    public ResponseEntity<String> update(@RequestBody KeyValuePair pair) throws IOException {
        SSTable.update(pair.getKey(), pair.getValue());
        return new ResponseEntity<>("Value Updated", HttpStatus.OK);
    }

    @DeleteMapping("/delete")
    public ResponseEntity<String> delete(@RequestParam String key) throws IOException {
        SSTable.delete(key);
        return new ResponseEntity<>("Value Deleted", HttpStatus.OK);
    }

    @GetMapping("/get")
    public ResponseEntity<String> read(@RequestParam String key) throws IOException {
        String value = SSTable.readKey(key);
        return new ResponseEntity<>("Value Read: "+ value, HttpStatus.OK);
    }

    @PutMapping("/recover")
    public ResponseEntity<String> recover() throws IOException {
        recoverLog.recover(LOG_FILE_NAME);
        SSTable.recoverFileNameAndSSTableMap();
        return new ResponseEntity<>("Recovered", HttpStatus.OK);
    }
}