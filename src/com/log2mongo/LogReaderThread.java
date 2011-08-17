package com.log2mongo;

import java.io.DataInput;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LogReaderThread extends Thread {
    private final DataInput logReader;
    private final BlockingQueue<String> logQueue;

    public LogReaderThread(DataInput logReader, BlockingQueue<String> logQueue) {
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
                Logger.getLogger(LogReaderThread.class.getName()).log(Level.SEVERE, null, e);

            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
