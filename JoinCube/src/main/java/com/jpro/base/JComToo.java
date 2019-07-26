package com.jpro.base;

import com.jpro.frame.JoinCube;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.io.File;
import java.io.FileInputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Log4j2
public class JComToo {

    public static LongAdder statisticOfDelete  = new LongAdder();
    public static LongAdder statisticOfInsert = new LongAdder();
    public static LongAdder statisticOfQuery  = new LongAdder();
    public static ReentrantReadWriteLock statisticLock  = new ReentrantReadWriteLock();
    public static long statisticOfLastTime = 0;

    private static JComToo jComToo;

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
        return prop;
    }

    public static Properties getGlobalContextF(String s) {
        Properties prop = new Properties();
        try {
            File fp = new File(s);
            if (!fp.exists()) {
                log.error("no such properties file");
                System.exit(-1);
            }
            FileInputStream inf = new FileInputStream(fp);
            prop.load(inf);
        } catch (Exception e) {
            log.error("Load file failed: " + e);
            System.exit(-1);
        }
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

    public static void sleep(double t) {
        try {
            Thread.sleep((long) (t * 1000));
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

//    public static long timestampToSecond(String timestamp) throws ParseException {
//        timestamp = timestamp.toUpperCase();
//        SimpleDateFormat timeFormat;
//        if (timestamp.length() == 19 && timestamp.charAt(10) == ' ') {
//            timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        } else if (timestamp.length() == 19 && timestamp.charAt(10) == 'T') {
//            timeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
//        } else if (timestamp.length() == 20 && timestamp.charAt(10) == 'T' && timestamp.charAt(19) == 'Z') {
//            timeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
//        } else {
//            throw new ParseException("unknown timestamp format", 0);
//        }
//        return timeFormat.parse(timestamp).getTime();
//    }

    public static void gatherDocument(Document to, Document from) {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        BiConsumer<String, Object> f = (k, v) -> {
            if (to.containsKey(k)) return;
            lock.writeLock().lock();
            to.put(k, v);
            lock.writeLock().unlock();
        };
        from.forEach(f);
    }

    public static void log(String msg) {
        System.out.println("[ TRACE ] " + msg);
    }
    public static void log(String msg, String src) {
        System.out.println("[ TRACE - " + src + " ] " + msg);
    }
//    public static void tlog(String msg) {
//        System.out.println("[ TRACE ] " + msg);
//    }
//    public static void tlog(String msg, String src) {
//        System.out.println("[ TRACE - " + src + " ] " + msg);
//    }
}
