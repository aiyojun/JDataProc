package com.jpro;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.jpro.Unique.Utils.gtv;

@Log4j2
public class Unique {

    static class Utils {
        /**
         * gtv: get value by key
         */
        public static String gtv(Properties p, String k) {
            return p.getProperty(k);
        }

        public static int gtv2int(Properties p, String k) {
            return Integer.parseInt(p.getProperty(k));
        }
    }

    public static AtomicBoolean working = new AtomicBoolean(true);

    public static Properties elfin;

    public static String mongoUrl;
    public static String mongoDB;

    public static Map<String, String> stationMapper;

    public static void load(String p) {
        if (elfin != null) return;
        elfin = new Properties();
        File fp = new File(p);
        if (!fp.exists()) {
            log.error("no such properties file");
            System.exit(-1);
        }
        try {
            elfin.load(new FileInputStream(fp));
        } catch (IOException e) {
            e.printStackTrace();
        }

        /**
         * import static info
         */
        mongoUrl = "mongodb://" + gtv(elfin, "mongo.url");
        mongoUrl = gtv(elfin, "mongo.database");

        MongoClient tmpMon = new MongoClient(mongoUrl);
        MongoCursor<Document> cursor =
                tmpMon.getDatabase(mongoDB).getCollection(gtv(elfin, "sys.process.table"))
                        .find().iterator();
        while (cursor.hasNext()) {
            Document ele = cursor.next();

        }
    }

    public static void initializeAllResource(String[] args) {
        if (args.length != 1) {
            log.error("Usage: JoinCube [ properties-file-path ]");
            System.exit(1);
        }
        log.info("\033[36;1minitialize resource\033[0m");
        load(args[0]);
    }

    public static void recycleAllResource() {
        log.info("\033[36;1mrecycle resource\033[0m");
    }

}
