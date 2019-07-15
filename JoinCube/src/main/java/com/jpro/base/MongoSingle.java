package com.jpro.base;

import com.mongodb.MongoClient;

import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MongoSingle {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static MongoSingle self;
    private MongoSingle() {}
    public static MongoSingle getInstance() {
        if (self == null) {
            self = new MongoSingle();
        }
        return self;
    }
    private MongoClient mongoClient;
    private Properties context;
    public void setContext(Properties prop) {
        if (context == null) {
            context = prop;
        }
    }
    public ReentrantReadWriteLock getLock() {
        return lock;
    }
    public MongoClient getMongoClient() {
        if (context == null) {
            return null;
        }
        if (mongoClient == null) {
            mongoClient = new MongoClient(context.getProperty("mongo.server.ip"),
                    Integer.parseInt(context.getProperty("mongo.server.port")));
        }
        return mongoClient;
    }
}
