package com.jpro.base;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class GlobalContext {
    public static AtomicBoolean isWorking = new AtomicBoolean(true);

    private static GlobalContext instance;
    private GlobalContext() { }
    public static GlobalContext getInstance() {
        if (instance == null) {
            instance = new GlobalContext();
        }
        return instance;
    }

    public static Properties context;
    public void setContext(Properties props) {
        context = props;
        mongoServerIp           = context.getProperty("mongo.server.ip");
        mongoServerPort         = Integer.parseInt(context.getProperty("mongo.server.port"));
        mongoDatabase           = context.getProperty("mongo.database");
        collectionOfAim         = context.getProperty("mongo.AIM.collection");
        collectionOfCnc         = context.getProperty("mongo.CNC.collection");
        collectionOfSn          = context.getProperty("mongo.SN.collection");
        collectionOfUnjoined    = context.getProperty("mongo.AIM.unjoined.collection");
        uniqueIdKeyOfAim        = context.getProperty("AIM.unique.key");
        uniqueIdKeyOfCnc        = context.getProperty("CNC.unique.key");
        uniqueIdKeyOfSn         = context.getProperty("SN.unique.key");
        stationKeyOfAim         = context.getProperty("AIM.station.key");
        processKeyOfAim         = context.getProperty("AIM.process.key");
    }
    public Properties getContext() {
        if (context == null) {
            throw new RuntimeException("please set context first");
        }
        return context;
    }

    public static String mongoServerIp;
    public static int mongoServerPort;
    public static String mongoDatabase;
    public static String collectionOfAim;
    public static String collectionOfUnjoined;
    public static String collectionOfCnc;
    public static String collectionOfSn;

    public static String uniqueIdKeyOfAim;
    public static String uniqueIdKeyOfCnc;
    public static String uniqueIdKeyOfSn;

    public static String stationKeyOfAim;
    public static String processKeyOfAim;

}
