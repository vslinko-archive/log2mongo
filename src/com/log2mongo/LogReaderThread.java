package com.log2mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.DataInput;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class LogReaderThread extends Thread {
    private final DBCollection errorCollection;
    private final DataInput logReader;
    private final BlockingQueue<String> logQueue;

    public LogReaderThread(DBCollection errorCollection, DataInput logReader, BlockingQueue<String> logQueue) {
        this.errorCollection = errorCollection;
        this.logReader = logReader;
        this.logQueue = logQueue;
    }

    @Override
    public void run() {
        String line;
        while (true) {
            try {
                while ((line = logReader.readLine()) == null) {
                    sleep(100);
                }

                logQueue.add(line);

            } catch (IOException e) {
                DBObject error = new BasicDBObject();
                error.put("class", e.getClass().getName());
                error.put("message", e.getMessage());
                errorCollection.insert(error);

            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
