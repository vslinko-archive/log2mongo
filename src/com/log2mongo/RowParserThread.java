package com.log2mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RowParserThread extends Thread {
    private static final Pattern pattern = Pattern.compile("^([^ ]+) - \\[([^\\]]+)\\] \"([^\"]+)\" \"([^ ]+)[^\"]*\" (\\d+) \\((\\d+)\\) \"([^\"]+)\" \"([^ ]+) ([^\"]+)\" \\[([.0-9]+)\\]$");
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ZZZZZ");

    private final DBCollection errorCollection;
    private final BlockingQueue<String> logQueue;
    private final BlockingQueue<BasicDBObject> docQueue;

    public RowParserThread(DBCollection errorCollection, BlockingQueue<String> logQueue, BlockingQueue<BasicDBObject> docQueue) {
        this.errorCollection = errorCollection;
        this.logQueue = logQueue;
        this.docQueue = docQueue;
    }

    @Override
    public void run() {
        String line;
        while (true) {
            try {
                while ((line = logQueue.poll()) == null) {
                    sleep(100);
                }

                BasicDBObject doc = parse(line);
                docQueue.put(doc);

            } catch (InterruptedException e) {
                break;

            } catch (InvalidLogFormatException e) {
                DBObject error = new BasicDBObject();
                error.put("class", e.getClass().getName());
                error.put("message", e.getMessage());
                errorCollection.insert(error);
            }
        }
    }

    private BasicDBObject parse(String line) throws InvalidLogFormatException {
        Matcher matcher = pattern.matcher(line);

        if (!matcher.matches()) {
            System.out.println(line);
            throw new InvalidLogFormatException(line);
        }

        try {
            BasicDBObject doc = new BasicDBObject();

            doc.put("ip", matcher.group(1));
            doc.put("date", dateFormat.parse(matcher.group(2)));
            doc.put("host", matcher.group(3));
            doc.put("method", matcher.group(4));
            doc.put("response_code", Integer.parseInt(matcher.group(5)));
            doc.put("length", Integer.parseInt(matcher.group(6)));
            doc.put("referer", matcher.group(7));
            doc.put("uri", matcher.group(8));
            doc.put("args", matcher.group(9));
            doc.put("request_time", Float.parseFloat(matcher.group(10)));

            return doc;

        } catch (ParseException e) {
            throw new InvalidLogFormatException(line, e);
        }
    }
}
