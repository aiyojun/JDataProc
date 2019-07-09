package com.jpro;

import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class CNCDataProc implements AbstDataProc {
    private Properties context;

    private List<String> optionValues;

    public CNCDataProc(Properties pro) {
        context = pro;
        optionValues = ComToo.parseArrayString(context.getProperty("option.field1.value1"));
    }

    @Override
    public Document doVerify(Document orig) {
        if (!orig.containsKey(context.getProperty("unique.field"))
                || !orig.containsKey(context.getProperty("option.field1"))
                || !orig.containsKey(context.getProperty("filter.field"))) {
            throw new RuntimeException("lack of necessary field");
        }
        return orig;
    }

    @Override
    public Document doFilter(Document orig) {
        log.info("CNCDataProc::doFilter");
//        orig.forEach((k, v) -> {
//            System.out.println(k + " - " + v);
//        });
//        if (!orig.containsKey(context.getProperty("unique.field"))) {
//            log.info("not contain " + orig.getString(""));
//        }
//        System.exit(5);
        if (orig.getString(context.getProperty("filter.field"))
                .contains(context.getProperty("filter.value"))) {
            return orig;
        }

        return null;
    }

    @Override
    public Document generateNotifyData(Document orig) {
        log.info("CNCDataProc::generateNotifyData");
        Document _r = new Document();

        optionValues.forEach((value) -> {
            if (orig.getString(context.getProperty("option.field1")).contains(value)) {
                _r.put(context.getProperty("unique.field"),
                        orig.getString(context.getProperty("unique.field")));
                _r.put(value + "_" + context.getProperty("option.field1.postfix"),
                        orig.getString(context.getProperty("option.field1")));
                _r.put(value + "_" + context.getProperty("option.field2.postfix"),
                        orig.getString(context.getProperty("option.field2")));
                _r.put(value + "_" + context.getProperty("option.field3.postfix"),
                        orig.getString(context.getProperty("option.field3")));
            }
        });
        return _r;
    }

    @Override
    public Document generateStorageData(Document orig) {
        log.info("CNCDataProc::generateStorageData");
//        StringBuilder uniqueID = new StringBuilder();
//        uniqueID.append(orig.getString(context.getProperty("unique.field")));
//
//        uniqueID.append("_");
//        AtomicBoolean isModified = new AtomicBoolean(false);
//        optionValues.forEach((value) -> {
//            if (orig.getString(context.getProperty("option.field1")).contains(value)) {
//                uniqueID.append(value);
//                isModified.set(true);
//            }
//        });
//        if (!isModified.get()) {
//            throw new RuntimeException("cannot match " + context.getProperty("option.field1.value1"));
//        }
        orig.put("_id", orig.getString(context.getProperty("unique.field")) + "_" + orig.getString(context.getProperty("option.field1")));
        return orig;
    }
}
