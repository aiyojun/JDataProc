package com.jpro;

import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.Map;
import java.util.Properties;

@Log4j2
public class SNDataProc implements AbstDataProc {
    Properties context;

    private Map<String, String> colorDict;

    SNDataProc(Properties pro) {
        context = pro;
        colorDict = ComToo.generateColorDict(context.getProperty("color.dict"));
    }

    @Override
    public Document doVerify(Document orig) {
        if (!orig.containsKey(context.getProperty("unique.field"))
                || !orig.containsKey(context.getProperty("option.field1"))
                || !orig.containsKey(context.getProperty("option.field2"))
                || !orig.containsKey(context.getProperty("option.field3"))
                || !orig.containsKey(context.getProperty("option.field4"))
        ) {
            throw new RuntimeException("lack of necessary field");
        }
        return orig;
    }

    @Override
    public Document doFilter(Document orig) {
        if (orig.containsKey(context.getProperty("color.field")) && orig.get(context.getProperty("color.field")) instanceof String) {
            String colorValue = orig.getString(context.getProperty("color.field"));
            if (colorDict.containsKey(colorValue)) {
                orig.put(context.getProperty("color.field"), colorDict.get(colorValue));
            } else {
                log.warn("No such color [ " + colorValue + "] in color dict.");
            }
        }
        return orig;
    }

    @Override
    public Document generateNotifyData(Document orig) {
        return orig;
    }

    @Override
    public Document generateStorageData(Document orig) {
        orig.put("_id", orig.getString(context.getProperty("unique.field")));
        return orig;
    }
}
