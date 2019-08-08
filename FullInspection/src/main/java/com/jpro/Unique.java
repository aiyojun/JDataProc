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
import static com.jpro.Unique.Utils.gtv2int;

@Log4j2
public class Unique {

    static class PartAttr {
        public String cartonVolume;
        public String meterialType;
        public String jancode;
        public String upccode;
        public PartAttr(String cartonVolume, String meterialType, String jancode, String upccode) {
            this.cartonVolume = cartonVolume;
            this.meterialType = meterialType;
            this.jancode = jancode;
            this.upccode = upccode;
        }
    }

    static class ProcessStruct {
        private String name;
        private String rework;
        public ProcessStruct(String name) {
            this.name = name;
        }
        public ProcessStruct(String name, String rework) {
            this.name = name;
            this.rework = rework;
        }

        public String getRework() {
            return rework;
        }

        public String getName() {
            return name;
        }
    }

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

    static class Url {
        public String url;
        public String ip;
        public int port;
        public Url(String ip, int port) {
            this.ip = ip;
            this.port = port;
            this.url = ip + port;
        }
    }

    public static AtomicBoolean working = new AtomicBoolean(true);

    public static Properties elfin;

    public static Url mongoUrl;
    public static String mongoDB;

    public static Map<String, ProcessStruct> stationMapper = new HashMap<>();
    public static Map<String, String> reworkMapper = new HashMap<>();
    public static Map<String, Character> buildMapper = new HashMap<>();
    public static Map<String, PartAttr> partMapper = new HashMap<>();

    public static String defectTab;
    public static String defectSysTab;
    public static String dotTab;

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
        mongoUrl    = new Url(gtv(elfin, "mongo.ip"), gtv2int(elfin, "mongo.port"));
        mongoDB     = gtv(elfin, "mongo.database");
        defectTab   = gtv(elfin, "defect.table");
        dotTab      = gtv(elfin, "cnc.table");
        defectSysTab= gtv(elfin, "sys.defect.table");



        /**
         * process mapper
         */
        String proccessTab = gtv(elfin, "sys.process.table");
        String proccessIDKey = gtv(elfin, "sys.process.id.key");
        String proccessNameKey = gtv(elfin, "sys.process.name.key");
        String proccessReworkKey = gtv(elfin, "sys.process.rework.key");
        MongoClient tmpMon = new MongoClient(mongoUrl.ip, mongoUrl.port);
        MongoCursor<Document> cursor =
                tmpMon.getDatabase(mongoDB).getCollection(proccessTab)
                        .find().iterator();
        while (cursor.hasNext()) {
            Document ele = cursor.next();
            if (ele.get(proccessIDKey) instanceof String && ele.get(proccessNameKey) instanceof String) {
                stationMapper.put(ele.getString(proccessIDKey),
                        new ProcessStruct(ele.getString(proccessNameKey), ele.getString(proccessReworkKey)));
                if (!ele.getString(proccessReworkKey).isEmpty())
                    reworkMapper.put(ele.getString(proccessReworkKey), ele.getString(proccessIDKey));
            }
        }

        /**
         * All build info
         */
        String buildTypes = gtv(elfin, "build.types");
        String[] builds = buildTypes.substring(1, buildTypes.length() - 1).split(",");
        for (int i = 0; i < builds.length; i++) {
            buildMapper.put(builds[i].replace("\"", ""), '0');
        }

        /**
         * load part table
         * --------------------------------------------
         * sys.part.product.key         = CARTON_VOLUME
         * sys.part.product.code.key    = MATERIAL_TYPE
         * sys.part.color.key           = JANCODE
         * sys.part.media.key           = UPCCODE
         * --------------------------------------------
         */
        String partTab          = gtv(elfin, "sys.part.table");
        String partIDKey        = gtv(elfin, "sys.part.id.key");
        String productKey       = gtv(elfin, "sys.part.product.key");
        String productCodeKey   = gtv(elfin, "sys.part.product.code.key");
        String colorKey         = gtv(elfin, "sys.part.color.key");
        String mediaKey         = gtv(elfin, "sys.part.media.key");
        cursor.close();
        cursor = tmpMon.getDatabase(mongoDB).getCollection(partTab)
                        .find().iterator();
        while (cursor.hasNext()) {
            Document ele = cursor.next();
            if (ele.get(partIDKey) instanceof String
                    && ele.get(productKey) instanceof String
                    && ele.get(productCodeKey) instanceof String
                    && ele.get(colorKey) instanceof String
                    && ele.get(mediaKey) instanceof String) {
                partMapper.put(ele.getString(partIDKey),
                        new PartAttr(
                                toJ320OrJ420(ele.getString(productCodeKey)),
                                ele.getString(productKey),
                                ele.getString(colorKey),
                                ele.getString(mediaKey)));
            }
        }
        cursor.close();
        tmpMon.close();
        log.info("load part dictionary table");
    }

    private static String toJ320OrJ420(String productCode) {
        String l = productCode.toLowerCase();
        if (l.contains("j320")) {
            return "j320";
        } else if (l.contains("j420")) {
            return "j420";
        } else {
            return "";
        }
    }


    public static void initializeAllResource(String[] args) {
        if (args.length != 1) {
            log.error("Usage: JoinCube [ properties-file-path ]");
            System.exit(1);
        }
        log.info("\033[36;1minitialize resource\033[0m");
        load(args[0]);
        log.info("\033[36;1m====================\033[0m");
        log.info("load config file info:");
        elfin.forEach((k, v) -> {log.info("    " + k + " : " + v);});
        log.info("\033[36;1m====================\033[0m");
        log.info("The following is build mapper:");
        buildMapper.forEach((k, v) -> log.info("    " + k));

    }

    public static void recycleAllResource() {
        Unique.working.set(false);
        Server.HDL.close();
        log.info("\033[36;1mrecycle resource\033[0m");
    }

}
