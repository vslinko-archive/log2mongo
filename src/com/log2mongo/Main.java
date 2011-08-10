package com.log2mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: java -jar log2mongo LOG_FILE_PATH");
            System.exit(1);
        }

        String filepath = args[0];

        try {
            // Source
            BufferedReader logReader = new BufferedReader(new FileReader(filepath));

            // Destination
            DBCollection collection = new Mongo().getDB("logging").getCollection("logs");

            // Queues
            BlockingQueue<String> logQueue = new LinkedBlockingQueue<String>();
            BlockingQueue<BasicDBObject> docQueue = new LinkedBlockingQueue<BasicDBObject>();

            // Threads
            Thread readerThread = new LogReaderThread(logReader, logQueue);
            Thread parserThread = new RowParserThread(logQueue, docQueue);
            Thread writerThread = new DocWriterThread(docQueue, collection);

            // Start
            readerThread.start();
            parserThread.start();
            writerThread.start();

        } catch (FileNotFoundException e) {
            System.err.println("File \"" + filepath + "\" not found");
            System.exit(2);

        } catch (UnknownHostException e) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, e);
        }
    }
}
