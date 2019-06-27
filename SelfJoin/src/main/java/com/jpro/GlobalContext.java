package com.jpro;

import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

@Log4j2
class GlobalContext {
    private Properties gProperties;

    private static GlobalContext unique;

    private GlobalContext() {}

    static synchronized GlobalContext getInstence() {
        if (unique == null) {
            unique = new GlobalContext();
        }
        return unique;
    }

    void load(String path) {
        if (gProperties != null) { return; }
        try {
            File fp = new File(path);
            if (!fp.exists()) {
                throw new RuntimeException("no properties file");
            }
            FileInputStream inf = new FileInputStream(fp);
            gProperties = new Properties();
            gProperties.load(inf);
            inf.close();
        } catch (Exception e) {
            log.error("Load file failed: " + e);
            System.exit(-1);
        }
    }

    Properties getProperties() {
        return gProperties;
    }
}
