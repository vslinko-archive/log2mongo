package com.log2mongo;

import com.martiansoftware.jsap.*;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    public static void main(String[] args) {
        JSAPResult config = parseArgs(args);
        String logFilePath = config.getString("logFilePath");

        try {
            // Source
            RandomAccessFile logReader = new RandomAccessFile(logFilePath, "r");
            logReader.seek(logReader.length());

            // Destination
            Mongo mongo = new Mongo(config.getString("host"), config.getInt("port"));
            DB db = mongo.getDB(config.getString("database"));
            DBCollection collection = db.getCollection(config.getString("collection"));
            DBCollection errorCollection = db.getCollection(config.getString("errorCollection"));

            // Queues
            BlockingQueue<String> logQueue = new LinkedBlockingQueue<String>();
            BlockingQueue<BasicDBObject> docQueue = new LinkedBlockingQueue<BasicDBObject>();

            // Threads
            Thread readerThread = new LogReaderThread(errorCollection, logReader, logQueue);
            Thread parserThread = new RowParserThread(errorCollection, logQueue, docQueue);
            Thread writerThread = new DocWriterThread(docQueue, collection);

            // Start
            readerThread.start();
            parserThread.start();
            writerThread.start();

        } catch (UnknownHostException e) {
            // Unknown localhost?!
            e.printStackTrace();

        } catch (IOException e) {
            System.err.println("File \"" + logFilePath + "\" not found");
            System.exit(2);
        }
    }

    private static JSAPResult parseArgs(String[] args) {
        JSAP jsap = new JSAP();

        UnflaggedOption logFilePathOption = new UnflaggedOption("logFilePath")
                .setStringParser(JSAP.STRING_PARSER)
                .setDefault("/var/log/nginx/access.log")
                .setRequired(false);

        FlaggedOption hostOption = new FlaggedOption("host")
                .setStringParser(JSAP.STRING_PARSER)
                .setDefault("127.0.0.1")
                .setShortFlag('h')
                .setRequired(false);

        FlaggedOption portOption = new FlaggedOption("port")
                .setStringParser(JSAP.INTEGER_PARSER)
                .setDefault("27017")
                .setShortFlag('p')
                .setRequired(false);

        FlaggedOption databaseOption = new FlaggedOption("database")
                .setStringParser(JSAP.STRING_PARSER)
                .setDefault("log2mongo")
                .setShortFlag('d')
                .setRequired(false);

        FlaggedOption collectionOption = new FlaggedOption("collection")
                .setStringParser(JSAP.STRING_PARSER)
                .setDefault("nginx")
                .setShortFlag('c')
                .setRequired(false);

        FlaggedOption errorCollectionOption = new FlaggedOption("errorCollection")
                .setStringParser(JSAP.STRING_PARSER)
                .setDefault("log2mongo-errors")
                .setShortFlag('e')
                .setRequired(false);

        try {
            jsap.registerParameter(logFilePathOption);
            jsap.registerParameter(hostOption);
            jsap.registerParameter(portOption);
            jsap.registerParameter(databaseOption);
            jsap.registerParameter(collectionOption);
            jsap.registerParameter(errorCollectionOption);
        } catch (JSAPException e) {
            e.printStackTrace();
        }

        return jsap.parse(args);
    }
}
