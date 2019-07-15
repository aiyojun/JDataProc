package com.jpro.base;

import com.jpro.frame.JoinCube;
import lombok.extern.log4j.Log4j2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Log4j2
public class JComToo {

    private static JComToo jComToo;

    public static Properties context;

    private String MongoDatabase;
    private String CNCCollection;
    private String CNCProcessKey;
    private String CNCCellKey;
    private String CNCMachineKey;
    private String SNCollection;
    public String getMongoDatabase() {
        if (MongoDatabase == null) {
            MongoDatabase = context.getProperty("mongo.database");
        }
        return MongoDatabase;
    }
    public String getCNCCollection() {
        if (CNCCollection == null) {
            CNCCollection = context.getProperty("mongo.CNC.collection");
        }
        return CNCCollection;
    }
    public String getCNCProcessKey() {
        if (CNCProcessKey == null) {
            CNCProcessKey = context.getProperty("CNC.process.key");
        }
        return CNCProcessKey;
    }
    public String getCNCCellKey() {
        if (CNCCellKey == null) {
            CNCCellKey = context.getProperty("CNC.cell.key");
        }
        return CNCCellKey;
    }
    public String getCNCMachineKey() {
        if (CNCMachineKey == null) {
            CNCMachineKey = context.getProperty("CNC.machine.key");
        }
        return CNCMachineKey;
    }
    public String getSNCollection() {
        if (SNCollection == null) {
            SNCollection = context.getProperty("mongo.SN.collection");
        }
        return SNCollection;
    }

    public JComToo() {
        pattern_1 = Pattern.compile(regex_1);
        pattern_2 = Pattern.compile(regex_2);
    }

    public static JComToo T() {
        if (jComToo == null) {
            jComToo = new JComToo();
        }
        return jComToo;
    }


    public static Properties getGlobalContext(String s) {
        Properties prop = new Properties();
        try {
            prop.load(JoinCube.class.getClassLoader()
                    .getResourceAsStream(s));
        } catch (Exception e) {
            log.error("Load file failed: " + e);
            System.exit(-1);
        }
        context = prop;
        return prop;
    }

    private Pattern pattern_1;
    private Pattern pattern_2;
    private final String regex_1 = "\\+?(\\d+)h";
    private final String regex_2 = "-(\\d+)h";
    public int parseTimeDiff(String diff) {
        int _r = 0;
        if (Pattern.matches(regex_1, diff)) {
            Matcher matcher = pattern_1.matcher(diff);
            if (matcher.find()) {
                _r = Integer.parseInt(matcher.group(1));
            }
        } else if (Pattern.matches(regex_2, diff)) {
            Matcher matcher = pattern_2.matcher(diff);
            if (matcher.find()) {
                _r = 0 - Integer.parseInt(matcher.group(1));
            }
        }
        return _r;
    }

    public String toTimestamp(Date t) {
        SimpleDateFormat timeFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return timeFormat.format(t);
    }
    public Date timestampToDate(String s) throws ParseException {
        SimpleDateFormat timeFormat =
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return timeFormat.parse(s);
    }

    public static void log(String msg) {
        System.out.println("[ TRACE ] " + msg);
    }
    public static void log(String msg, String src) {
        System.out.println("[ TRACE - " + src + " ] " + msg);
    }
    public static void tlog(String msg) {
        System.out.println("[ TRACE ] " + msg);
    }
    public static void tlog(String msg, String src) {
        System.out.println("[ TRACE - " + src + " ] " + msg);
    }
}
