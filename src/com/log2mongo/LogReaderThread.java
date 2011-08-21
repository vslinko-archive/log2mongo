package com.log2mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;

public class LogReaderThread extends Thread {
    private final DBCollection errorCollection;
    private final String logFilePath;
    private final BlockingQueue<String> logQueue;
    private RandomAccessFile logReader;

    public LogReaderThread(DBCollection errorCollection, String logFilePath, BlockingQueue<String> logQueue) throws IOException {
        this.errorCollection = errorCollection;
        this.logFilePath = logFilePath;
        this.logQueue = logQueue;

        logReader = new RandomAccessFile(logFilePath, "r");
        logReader.seek(logReader.length());
    }

    @Override
    public void run() {
        String line;
        short sleepCount = 0;
        while (true) {
            try {
                while ((line = logReader.readLine()) == null) {
                    sleep(100);
                    sleepCount += 1;

                    if (sleepCount % 10 == 0) {
                        refreshReaderFile();
                    }
                }
                sleepCount = 0;

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

    private void refreshReaderFile() throws IOException {
        RandomAccessFile newLogReader = new RandomAccessFile(logFilePath, "r");

        if (newLogReader.length() != logReader.length()) {
            logReader = newLogReader;
        }
    }
}
