package com.jpro;

import org.bson.Document;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class CNCDataProc implements AbstDataProc {
    private Properties context;

    private List<String> optionValues;

    public CNCDataProc(Properties pro) {
        context = pro;
        optionValues = ComToo.parseArrayString(context.getProperty("option.field1.value1"));
    }

    @Override
    public Document doFilter(Document orig) {
        if (!orig.containsKey("unique.field")
                || !orig.containsKey("option.field1")
                || !orig.containsKey("filter.field")) {
            throw new RuntimeException("lack of necessary field");
        }
        if (orig.getString(context.getProperty("option.field"))
                .contains(context.getProperty("filter.value"))) {
            return orig;
        }
        return null;
    }

    @Override
    public Document generateNotifyData(Document orig) {
        Document _r = new Document();

        optionValues.forEach((value) -> {
            if (orig.getString(context.getProperty("option.field1")).contains(value)) {
                _r.put(value + context.getProperty("option.field1.postfix"),
                        orig.getString(context.getProperty("option.field1")));
                _r.put(value + context.getProperty("option.field2.postfix"),
                        orig.getString(context.getProperty("option.field2")));
                _r.put(value + context.getProperty("option.field3.postfix"),
                        orig.getString(context.getProperty("option.field3")));
            }
        });
        return _r;
    }

    @Override
    public Document generateStorageData(Document orig) {
        StringBuilder uniqueID = new StringBuilder();
        uniqueID.append(orig.getString(context.getProperty("unique.field")));
        uniqueID.append("_");
        AtomicBoolean isModified = new AtomicBoolean(false);
        optionValues.forEach((value) -> {
            if (orig.getString(context.getProperty("option.field1")).contains(value)) {
                uniqueID.append(value);
                isModified.set(true);
            }
        });
        if (!isModified.get()) {
            throw new RuntimeException("cannot match " + context.getProperty("option.field1.value1"));
        }
        orig.put("_id", uniqueID);
        return orig;
    }
}
