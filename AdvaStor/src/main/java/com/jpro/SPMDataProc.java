package com.jpro;

import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.List;
import java.util.Properties;

@Log4j2
public class SPMDataProc implements AbstDataProc {
    Properties context;

//    private List<String> optionValues;

    SPMDataProc(Properties pro) {
        context = pro;
//        optionValues = ComToo.parseArrayString(context.getProperty("option.field1.value1"));
    }

    @Override
    public Document doVerify(Document orig) {
        if (!orig.containsKey(context.getProperty("unique.field"))
                || !orig.containsKey(context.getProperty("option.field1"))) {
            throw new RuntimeException("lack of necessary field");
        }
        return orig;
    }

    @Override
    public Document doFilter(Document orig) {
        return orig;
    }

    @Override
    public Document generateNotifyData(Document orig) {
        return orig;
    }

    @Override
    public Document generateStorageData(Document orig) {
        log.info("SPMDataProc::generateStorageData");
//        StringBuilder uniqueID = new StringBuilder();
//        uniqueID.append(orig.getString(context.getProperty("unique.field")));
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
//        orig.put("_id", uniqueID.toString());
        orig.put("_id", ComToo.hashAlgo(orig.getString(context.getProperty("unique.field")) + orig.getString(context.getProperty("option.field1"))));
        return orig;
    }
}
