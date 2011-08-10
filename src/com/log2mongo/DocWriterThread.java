package com.log2mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

import java.util.concurrent.BlockingQueue;

public class DocWriterThread extends Thread {
    private final BlockingQueue<BasicDBObject> docQueue;
    private final DBCollection collection;

    public DocWriterThread(BlockingQueue<BasicDBObject> docQueue, DBCollection collection) {
        this.docQueue = docQueue;
        this.collection = collection;
    }

    @Override
    public void run() {
        BasicDBObject doc;
        while (true) {
            try {
                while ((doc = docQueue.poll()) == null) {
                    sleep(100);
                }

                collection.insert(doc);

            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
